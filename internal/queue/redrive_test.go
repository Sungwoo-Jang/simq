package queue_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"simq/internal/queue"
	"simq/internal/storage/boltrepo"
)

type redriveTestClock struct{ now time.Time }

func (c redriveTestClock) Now() time.Time { return c.now }

func redriveRepositories(t *testing.T) map[string]func(*testing.T) queue.Repository {
	return map[string]func(*testing.T) queue.Repository{
		"memory": func(t *testing.T) queue.Repository { return queue.NewMemoryRepository() },
		"bbolt": func(t *testing.T) queue.Repository {
			repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(t.TempDir(), "simq.db")})
			if err != nil {
				t.Fatal(err)
			}
			return repository
		},
	}
}

func createRedriveQueue(t *testing.T, service *queue.Service, name string) queue.Queue {
	t.Helper()
	value, err := service.CreateQueue(queue.CreateQueueInput{QueueName: name, Attributes: map[string]string{"VisibilityTimeout": "0"}})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func setRedrive(t *testing.T, service *queue.Service, source, target queue.Queue, count int) {
	t.Helper()
	policy := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":%q}`, queue.QueueARN(queue.QueueRef{Name: target.Name, ID: target.ID}), fmt.Sprint(count))
	if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: source.Name, QueueID: source.ID, Attributes: map[string]string{"RedrivePolicy": policy}}); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticRedriveThresholdAndPayloadConformance(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := queue.NewService(repository, queue.WithClock(redriveTestClock{now: now}), queue.WithMessageIDGenerator(func() string { return "redrive-message" }))
			source := createRedriveQueue(t, service, "source")
			dlq := createRedriveQueue(t, service, "dead")
			setRedrive(t, service, source, dlq, 1)
			sent, err := service.SendMessage(queue.SendMessageInput{QueueName: source.Name, QueueID: source.ID, MessageBody: "payload", MessageAttributes: map[string]queue.MessageAttribute{"kind": {DataType: "String", StringValue: "order"}}})
			if err != nil {
				t.Fatal(err)
			}
			zero := 0
			first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero})
			if err != nil || len(first) != 1 || first[0].ReceiveCount != 1 {
				t.Fatalf("first receive = %#v, %v", first, err)
			}
			second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero})
			if err != nil || len(second) != 0 {
				t.Fatalf("threshold receive = %#v, %v", second, err)
			}
			if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: source.Name, QueueID: source.ID, ReceiptHandle: first[0].ReceiptHandle}); err != nil {
				t.Fatalf("stale pre-move receipt = %v", err)
			}
			moved, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: dlq.Name, QueueID: dlq.ID, MessageAttributeNames: []string{"All"}})
			if err != nil || len(moved) != 1 {
				t.Fatalf("DLQ receive = %#v, %v", moved, err)
			}
			if moved[0].ID != sent.ID || moved[0].Body != sent.Body || moved[0].MessageAttributes["kind"].StringValue != "order" || moved[0].SentAtMillis != sent.SentAtMillis || moved[0].ReceiveCount != 1 || moved[0].ReceiveGeneration <= first[0].ReceiveGeneration {
				t.Fatalf("moved message = %#v, sent = %#v", moved[0], sent)
			}
		})
	}
}

func TestDeadLetterSourcePaginationInvalidatesOnPolicyChange(t *testing.T) {
	repository := queue.NewMemoryRepository()
	defer repository.Close()
	service := queue.NewService(repository)
	target := createRedriveQueue(t, service, "dead")
	for _, name := range []string{"alpha", "beta", "gamma"} {
		setRedrive(t, service, createRedriveQueue(t, service, name), target, 2)
	}
	two := 2
	page, token, err := service.ListDeadLetterSourceQueues(queue.ListDeadLetterSourcesInput{Target: queue.QueueRef{Name: target.Name, ID: target.ID}, MaxResults: &two})
	if err != nil || len(page.Queues) != 2 || token == "" || page.Queues[0].Name != "alpha" || page.Queues[1].Name != "beta" {
		t.Fatalf("page = %#v token=%q err=%v", page, token, err)
	}
	alpha, _ := service.GetQueue(queue.GetQueueInput{QueueName: "alpha"})
	if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: alpha.Name, QueueID: alpha.ID, Attributes: map[string]string{"RedrivePolicy": ""}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.ListDeadLetterSourceQueues(queue.ListDeadLetterSourcesInput{Target: queue.QueueRef{Name: target.Name, ID: target.ID}, MaxResults: &two, NextToken: token}); err != queue.ErrInvalidPaginationToken {
		t.Fatalf("stale token error = %v", err)
	}
}

func TestDurableMoveTaskReturnsMessagesToOriginalSource(t *testing.T) {
	now := time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := queue.NewService(repository, queue.WithClock(redriveTestClock{now: now}), queue.WithMessageIDGenerator(func() string { return "move-message" }))
			source := createRedriveQueue(t, service, "source")
			dlq := createRedriveQueue(t, service, "dead")
			setRedrive(t, service, source, dlq, 1)
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: source.Name, QueueID: source.ID, MessageBody: "payload"}); err != nil {
				t.Fatal(err)
			}
			zero := 0
			if _, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero}); err != nil {
				t.Fatal(err)
			}
			if messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero}); err != nil || len(messages) != 0 {
				t.Fatalf("redrive receive = %#v, %v", messages, err)
			}
			redrive := repository.(queue.RedriveRepository)
			task, err := redrive.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_test", Source: queue.QueueRef{Name: dlq.Name, ID: dlq.ID}, MaxMessagesPerSecond: 500, Now: now})
			if err != nil || task.Status != queue.MoveTaskRunning || task.ApproximateNumberOfMessagesToMove != 1 {
				t.Fatalf("start = %#v, %v", task, err)
			}
			step, err := redrive.MoveTaskStep(task.Handle, now)
			if err != nil || step.Terminal || step.Task.ApproximateNumberOfMessagesMoved != 1 {
				t.Fatalf("step = %#v, %v", step, err)
			}
			completed, err := redrive.MoveTaskStep(task.Handle, now)
			if err != nil || !completed.Terminal || completed.Task.Status != queue.MoveTaskCompleted {
				t.Fatalf("completion = %#v, %v", completed, err)
			}
			moved, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID})
			if err != nil || len(moved) != 1 || moved[0].ID != "move-message" || moved[0].SentAtMillis != now.UnixMilli() {
				t.Fatalf("returned message = %#v, %v", moved, err)
			}
		})
	}
}

func TestMoveTaskCancellationIsDurableState(t *testing.T) {
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := queue.NewService(repository)
			source := createRedriveQueue(t, service, "source")
			dlq := createRedriveQueue(t, service, "dead")
			setRedrive(t, service, source, dlq, 1)
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: dlq.Name, QueueID: dlq.ID, MessageBody: "manual"}); err != nil {
				t.Fatal(err)
			}
			redrive := repository.(queue.RedriveRepository)
			task, err := redrive.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_cancel", Source: queue.QueueRef{Name: dlq.Name, ID: dlq.ID}, Destination: &queue.QueueRef{Name: source.Name, ID: source.ID}, MaxMessagesPerSecond: 1, Now: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			cancelled, err := redrive.CancelMoveTask(task.Handle, time.Now().UTC())
			if err != nil || cancelled.Status != queue.MoveTaskCancelled || cancelled.ApproximateNumberOfMessagesMoved != 0 {
				t.Fatalf("cancel = %#v, %v", cancelled, err)
			}
			step, err := redrive.MoveTaskStep(task.Handle, time.Now().UTC())
			if err != nil || !step.Terminal || step.Task.Status != queue.MoveTaskCancelled {
				t.Fatalf("post-cancel step = %#v, %v", step, err)
			}
		})
	}
}

func TestConcurrentThresholdReceivesMoveExactlyOneCopy(t *testing.T) {
	now := time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := queue.NewService(repository, queue.WithClock(redriveTestClock{now: now}), queue.WithMessageIDGenerator(func() string { return "concurrent-redrive" }))
			source := createRedriveQueue(t, service, "source")
			dlq := createRedriveQueue(t, service, "dead")
			setRedrive(t, service, source, dlq, 1)
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: source.Name, QueueID: source.ID, MessageBody: "once"}); err != nil {
				t.Fatal(err)
			}
			zero := 0
			if messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero}); err != nil || len(messages) != 1 {
				t.Fatalf("initial receive = %#v, %v", messages, err)
			}
			var wait sync.WaitGroup
			errorsFound := make(chan error, 16)
			for range 16 {
				wait.Add(1)
				go func() {
					defer wait.Done()
					messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero})
					if err != nil {
						errorsFound <- err
						return
					}
					if len(messages) != 0 {
						errorsFound <- fmt.Errorf("source returned %d messages", len(messages))
					}
				}()
			}
			wait.Wait()
			close(errorsFound)
			for err := range errorsFound {
				t.Error(err)
			}
			ten := 10
			messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: dlq.Name, QueueID: dlq.ID, MaxNumberOfMessages: &ten})
			if err != nil || len(messages) != 1 || messages[0].ID != "concurrent-redrive" {
				t.Fatalf("DLQ copies = %#v, %v", messages, err)
			}
		})
	}
}

func TestRedrivePolicyRejectsCyclesDuplicatesAndStaleTargets(t *testing.T) {
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := queue.NewService(repository)
			alpha := createRedriveQueue(t, service, "alpha")
			beta := createRedriveQueue(t, service, "beta")
			setRedrive(t, service, alpha, beta, 2)
			cycle := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"2"}`, queue.QueueARN(queue.QueueRef{Name: alpha.Name, ID: alpha.ID}))
			if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: beta.Name, QueueID: beta.ID, Attributes: map[string]string{"RedrivePolicy": cycle}}); err == nil {
				t.Fatal("cycle was accepted")
			}
			duplicate := fmt.Sprintf(`{"deadLetterTargetArn":%q,"deadLetterTargetArn":%q,"maxReceiveCount":"2"}`, queue.QueueARN(queue.QueueRef{Name: beta.Name, ID: beta.ID}), queue.QueueARN(queue.QueueRef{Name: beta.Name, ID: beta.ID}))
			if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: alpha.Name, QueueID: alpha.ID, Attributes: map[string]string{"RedrivePolicy": duplicate}}); err == nil {
				t.Fatal("duplicate policy field was accepted")
			}
			if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: alpha.Name, QueueID: alpha.ID, Attributes: map[string]string{"RedrivePolicy": ""}}); err != nil {
				t.Fatal(err)
			}
			if err := service.DeleteQueue(queue.QueueRef{Name: beta.Name, ID: beta.ID}); err != nil {
				t.Fatal(err)
			}
			recreated := createRedriveQueue(t, service, "beta")
			if recreated.ID == beta.ID {
				t.Fatal("queue generation did not change")
			}
			stale := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"2"}`, queue.QueueARN(queue.QueueRef{Name: beta.Name, ID: beta.ID}))
			if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: alpha.Name, QueueID: alpha.ID, Attributes: map[string]string{"RedrivePolicy": stale}}); err != queue.ErrQueueDoesNotExist {
				t.Fatalf("stale target error = %v", err)
			}
		})
	}
}

func TestMoveTaskBoundaryAndMissingOriginFailureConformance(t *testing.T) {
	now := time.Date(2026, 8, 25, 17, 0, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			sequence := 0
			service := queue.NewService(repository, queue.WithClock(redriveTestClock{now: now}), queue.WithMessageIDGenerator(func() string { sequence++; return fmt.Sprintf("boundary-%d", sequence) }))
			source := createRedriveQueue(t, service, "source")
			dlq := createRedriveQueue(t, service, "dead")
			setRedrive(t, service, source, dlq, 1)
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: dlq.Name, QueueID: dlq.ID, MessageBody: "before"}); err != nil {
				t.Fatal(err)
			}
			redrive := repository.(queue.RedriveRepository)
			task, err := redrive.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_boundary", Source: queue.QueueRef{Name: dlq.Name, ID: dlq.ID}, Destination: &queue.QueueRef{Name: source.Name, ID: source.ID}, MaxMessagesPerSecond: 500, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := redrive.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_duplicate", Source: task.Source, Destination: task.Destination, MaxMessagesPerSecond: 500, Now: now}); err != queue.ErrMoveTaskAlreadyRunning {
				t.Fatalf("second active task error = %v", err)
			}
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: dlq.Name, QueueID: dlq.ID, MessageBody: "after"}); err != nil {
				t.Fatal(err)
			}
			if _, err := redrive.MoveTaskStep(task.Handle, now); err != nil {
				t.Fatal(err)
			}
			completed, err := redrive.MoveTaskStep(task.Handle, now)
			if err != nil || completed.Task.Status != queue.MoveTaskCompleted {
				t.Fatalf("boundary completion = %#v, %v", completed, err)
			}
			moved, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID})
			if err != nil || len(moved) != 1 || moved[0].Body != "before" {
				t.Fatalf("moved boundary set = %#v, %v", moved, err)
			}
			remaining, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: dlq.Name, QueueID: dlq.ID})
			if err != nil || len(remaining) != 1 || remaining[0].Body != "after" {
				t.Fatalf("post-boundary arrivals = %#v, %v", remaining, err)
			}
			zero := 0
			if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{QueueName: dlq.Name, QueueID: dlq.ID, ReceiptHandle: remaining[0].ReceiptHandle, VisibilityTimeout: zero}); err != nil {
				t.Fatal(err)
			}
			failedTask, err := redrive.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_missing_origin", Source: task.Source, MaxMessagesPerSecond: 500, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			failed, err := redrive.MoveTaskStep(failedTask.Handle, now)
			if err != nil || failed.Task.Status != queue.MoveTaskFailed || failed.Task.FailureReason == "" || !failed.Terminal {
				t.Fatalf("missing-origin failure = %#v, %v", failed, err)
			}
		})
	}
}
