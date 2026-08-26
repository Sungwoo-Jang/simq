package queue_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simq/internal/queue"
)

type fifoTestClock struct{ nanos atomic.Int64 }

func newFIFOTestClock(now time.Time) *fifoTestClock {
	clock := &fifoTestClock{}
	clock.nanos.Store(now.UnixNano())
	return clock
}

func (c *fifoTestClock) Now() time.Time    { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *fifoTestClock) Set(now time.Time) { c.nanos.Store(now.UnixNano()) }

func newFIFOService(repository queue.Repository, clock *fifoTestClock) *queue.Service {
	var messages atomic.Uint64
	return queue.NewService(repository,
		queue.WithClock(clock),
		queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("fifo-message-%d", messages.Add(1)) }),
	)
}

func createFIFOQueue(t *testing.T, service *queue.Service, name string, contentBased bool) queue.Queue {
	t.Helper()
	attributes := map[string]string{"FifoQueue": "true", "VisibilityTimeout": "30"}
	if contentBased {
		attributes["ContentBasedDeduplication"] = "true"
	}
	value, err := service.CreateQueue(queue.CreateQueueInput{QueueName: name, Attributes: attributes})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sendFIFO(t *testing.T, service *queue.Service, value queue.Queue, body, group, dedup string) queue.Message {
	t.Helper()
	message, err := service.SendMessage(queue.SendMessageInput{QueueName: value.Name, QueueID: value.ID, MessageBody: body, MessageGroupID: group, MessageDeduplicationID: dedup})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestFIFOQueueConfigurationAndDeduplicationConformance(t *testing.T) {
	start := time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			clock := newFIFOTestClock(start)
			service := newFIFOService(repository, clock)

			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "missing-suffix", Attributes: map[string]string{"FifoQueue": "true"}}); err == nil {
				t.Fatal("FIFO queue without .fifo suffix was accepted")
			}
			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "reserved.fifo"}); err == nil {
				t.Fatal(".fifo name without FifoQueue=true was accepted")
			}
			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "standard", Attributes: map[string]string{"ContentBasedDeduplication": "true"}}); err == nil {
				t.Fatal("content-based deduplication on Standard queue was accepted")
			}

			fifo := createFIFOQueue(t, service, "orders.fifo", false)
			attributes, err := service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: fifo.Name, QueueID: fifo.ID, AttributeNames: []string{"All"}})
			if err != nil || attributes["FifoQueue"] != "true" || attributes["ContentBasedDeduplication"] != "false" {
				t.Fatalf("FIFO attributes = %#v, %v", attributes, err)
			}
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "body", MessageDeduplicationID: "d"}); err == nil {
				t.Fatal("FIFO send without group was accepted")
			}
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "body", MessageGroupID: "g"}); err == nil {
				t.Fatal("FIFO send without deduplication ID was accepted")
			}

			first := sendFIFO(t, service, fifo, "body", "group", "dedup")
			duplicate := sendFIFO(t, service, fifo, "different-body", "other-group", "dedup")
			if duplicate.ID != first.ID || duplicate.SequenceNumber != first.SequenceNumber || duplicate.MessageGroupID != first.MessageGroupID {
				t.Fatalf("duplicate = %#v, first = %#v", duplicate, first)
			}
			if first.SequenceNumber != 1 {
				t.Fatalf("first sequence = %d, want 1", first.SequenceNumber)
			}

			one := 1
			received, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MaxNumberOfMessages: &one})
			if err != nil || len(received) != 1 || received[0].ID != first.ID {
				t.Fatalf("receive = %#v, %v", received, err)
			}
			if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiptHandle: received[0].ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
			afterDelete := sendFIFO(t, service, fifo, "body-after-delete", "group", "dedup")
			if afterDelete.ID != first.ID || afterDelete.SequenceNumber != 1 {
				t.Fatalf("dedup after delete = %#v", afterDelete)
			}
			clock.Set(start.Add(5 * time.Minute))
			afterExpiry := sendFIFO(t, service, fifo, "new-body", "group", "dedup")
			if afterExpiry.ID == first.ID || afterExpiry.SequenceNumber != 2 {
				t.Fatalf("dedup at exact expiry = %#v", afterExpiry)
			}
		})
	}
}

func TestFIFOContentBasedDeduplicationUsesBodyOnly(t *testing.T) {
	start := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := newFIFOService(repository, newFIFOTestClock(start))
			fifo := createFIFOQueue(t, service, "content.fifo", true)
			first, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "same", MessageGroupID: "g", MessageAttributes: map[string]queue.MessageAttribute{"version": {DataType: "Number", StringValue: "1"}}})
			if err != nil {
				t.Fatal(err)
			}
			second, err := service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: "same", MessageGroupID: "other", MessageAttributes: map[string]queue.MessageAttribute{"version": {DataType: "Number", StringValue: "2"}}})
			if err != nil || second.ID != first.ID || second.SequenceNumber != first.SequenceNumber {
				t.Fatalf("content duplicate = %#v, first = %#v, err = %v", second, first, err)
			}
			explicit := sendFIFO(t, service, fifo, "same", "g", "explicit")
			if explicit.ID == first.ID || explicit.SequenceNumber != 2 {
				t.Fatalf("explicit dedup override = %#v", explicit)
			}
		})
	}
}

func TestFIFOGroupHeadsBlockAndIndependentGroupsProgress(t *testing.T) {
	start := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := newFIFOService(repository, newFIFOTestClock(start))
			fifo := createFIFOQueue(t, service, "groups.fifo", false)
			a1 := sendFIFO(t, service, fifo, "a1", "a", "a1")
			a2 := sendFIFO(t, service, fifo, "a2", "a", "a2")
			b1 := sendFIFO(t, service, fifo, "b1", "b", "b1")
			ten := 10
			first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MaxNumberOfMessages: &ten})
			if err != nil || len(first) != 2 || first[0].ID != a1.ID || first[1].ID != b1.ID {
				t.Fatalf("first group receive = %#v, %v", first, err)
			}
			if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiptHandle: first[1].ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
			blocked, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MaxNumberOfMessages: &ten})
			if err != nil || len(blocked) != 0 {
				t.Fatalf("blocked receive = %#v, %v", blocked, err)
			}
			if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiptHandle: first[0].ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
			next, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID})
			if err != nil || len(next) != 1 || next[0].ID != a2.ID || next[0].SequenceNumber <= a1.SequenceNumber {
				t.Fatalf("next group receive = %#v, %v", next, err)
			}
		})
	}
}

func TestFIFOConcurrentDeduplicationCommitsOneMessage(t *testing.T) {
	start := time.Date(2026, 8, 25, 15, 30, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := newFIFOService(repository, newFIFOTestClock(start))
			fifo := createFIFOQueue(t, service, "concurrent.fifo", false)
			const workers = 64
			results := make([]queue.Message, workers)
			errors := make([]error, workers)
			var wait sync.WaitGroup
			wait.Add(workers)
			for index := 0; index < workers; index++ {
				go func(index int) {
					defer wait.Done()
					results[index], errors[index] = service.SendMessage(queue.SendMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MessageBody: fmt.Sprintf("body-%d", index), MessageGroupID: "group", MessageDeduplicationID: "one"})
				}(index)
			}
			wait.Wait()
			for index := range results {
				if errors[index] != nil || results[index].ID != results[0].ID || results[index].SequenceNumber != 1 {
					t.Fatalf("result[%d] = %#v, %v; first = %#v", index, results[index], errors[index], results[0])
				}
			}
			ten := 10
			messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, MaxNumberOfMessages: &ten})
			if err != nil || len(messages) != 1 {
				t.Fatalf("committed messages = %#v, %v", messages, err)
			}
		})
	}
}

func TestFIFOPurgePreservesDedupAndSequence(t *testing.T) {
	start := time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			clock := newFIFOTestClock(start)
			service := newFIFOService(repository, clock)
			fifo := createFIFOQueue(t, service, "purge.fifo", false)
			first := sendFIFO(t, service, fifo, "one", "g", "dedup")
			if err := service.PurgeQueue(queue.QueueRef{Name: fifo.Name, ID: fifo.ID}); err != nil {
				t.Fatal(err)
			}
			duplicate := sendFIFO(t, service, fifo, "ignored", "g", "dedup")
			if duplicate.ID != first.ID || duplicate.SequenceNumber != first.SequenceNumber {
				t.Fatalf("dedup after purge = %#v, first = %#v", duplicate, first)
			}
			clock.Set(start.Add(5 * time.Minute))
			second := sendFIFO(t, service, fifo, "two", "g", "dedup")
			if second.ID == first.ID || second.SequenceNumber != 2 {
				t.Fatalf("sequence after purge = %#v", second)
			}
			if err := repository.Health(); err != nil {
				t.Fatalf("health after FIFO purge = %v", err)
			}
		})
	}
}

func TestFIFOReceiveAttemptReplaysSnapshotAndExpiresExactly(t *testing.T) {
	start := time.Date(2026, 8, 25, 16, 30, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			clock := newFIFOTestClock(start)
			service := newFIFOService(repository, clock)
			fifo := createFIFOQueue(t, service, "attempt.fifo", false)
			firstSent := sendFIFO(t, service, fifo, "one", "g", "one")
			secondSent := sendFIFO(t, service, fifo, "two", "g", "two")
			first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiveRequestAttemptID: "attempt-1"})
			if err != nil || len(first) != 1 || first[0].ID != firstSent.ID || first[0].ReceiveCount != 1 {
				t.Fatalf("first attempt = %#v, %v", first, err)
			}
			if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiptHandle: first[0].ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
			replay, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiveRequestAttemptID: "attempt-1"})
			if err != nil || !reflect.DeepEqual(replay, first) {
				t.Fatalf("attempt replay = %#v, first = %#v, err = %v", replay, first, err)
			}
			clock.Set(start.Add(5 * time.Minute))
			afterExpiry, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, ReceiveRequestAttemptID: "attempt-1"})
			if err != nil || len(afterExpiry) != 1 || afterExpiry[0].ID != secondSent.ID || afterExpiry[0].ReceiptHandle == first[0].ReceiptHandle {
				t.Fatalf("attempt at exact expiry = %#v, %v", afterExpiry, err)
			}
		})
	}
}

func TestFIFORedriveAndMoveAllocateDestinationSequences(t *testing.T) {
	start := time.Date(2026, 8, 25, 17, 0, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			service := newFIFOService(repository, newFIFOTestClock(start))
			source := createFIFOQueue(t, service, "source.fifo", false)
			dlq := createFIFOQueue(t, service, "dead.fifo", false)
			standard, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "standard"})
			if err != nil {
				t.Fatal(err)
			}
			mismatchedPolicy := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"1"}`, queue.QueueARN(queue.QueueRef{Name: standard.Name, ID: standard.ID}))
			if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: source.Name, QueueID: source.ID, Attributes: map[string]string{"RedrivePolicy": mismatchedPolicy}}); err == nil {
				t.Fatal("FIFO-to-Standard redrive policy was accepted")
			} else {
				var invalid *queue.InvalidRequestError
				if !errors.As(err, &invalid) {
					t.Fatalf("mismatched redrive error = %v", err)
				}
			}
			setRedrive(t, service, source, dlq, 1)
			redrive := repository.(queue.RedriveRepository)
			if _, err := redrive.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_cross_type", Source: queue.QueueRef{Name: dlq.Name, ID: dlq.ID}, Destination: &queue.QueueRef{Name: standard.Name, ID: standard.ID}, MaxMessagesPerSecond: 500, Now: start}); err == nil {
				t.Fatal("FIFO-to-Standard move task was accepted")
			}
			sent := sendFIFO(t, service, source, "payload", "group", "dedup")
			zero := 0
			first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero})
			if err != nil || len(first) != 1 {
				t.Fatalf("source receive = %#v, %v", first, err)
			}
			if messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero}); err != nil || len(messages) != 0 {
				t.Fatalf("redrive transition = %#v, %v", messages, err)
			}
			moved, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: dlq.Name, QueueID: dlq.ID, VisibilityTimeout: &zero})
			if err != nil || len(moved) != 1 || moved[0].ID != sent.ID || moved[0].MessageGroupID != "group" || moved[0].MessageDeduplicationID != "dedup" || moved[0].SequenceNumber != 1 {
				t.Fatalf("DLQ message = %#v, %v", moved, err)
			}
			task, err := redrive.StartMoveTask(queue.StartMoveTaskCommand{Handle: "mt_fifo", Source: queue.QueueRef{Name: dlq.Name, ID: dlq.ID}, MaxMessagesPerSecond: 500, Now: start})
			if err != nil {
				t.Fatal(err)
			}
			if step, err := redrive.MoveTaskStep(task.Handle, start); err != nil || step.Task.ApproximateNumberOfMessagesMoved != 1 {
				t.Fatalf("move step = %#v, %v", step, err)
			}
			returned, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: source.Name, QueueID: source.ID, VisibilityTimeout: &zero})
			if err != nil || len(returned) != 1 || returned[0].ID != sent.ID || returned[0].SequenceNumber != 2 || returned[0].MessageGroupID != "group" {
				t.Fatalf("returned message = %#v, %v", returned, err)
			}
			if err := repository.Health(); err != nil {
				t.Fatalf("health after FIFO redrive and move = %v", err)
			}
		})
	}
}

func TestFIFOLongPollDoesNotCommitEmptyAttemptBeforeCompletion(t *testing.T) {
	start := time.Date(2026, 8, 25, 17, 30, 0, 0, time.UTC)
	for name, open := range redriveRepositories(t) {
		t.Run(name, func(t *testing.T) {
			repository := open(t)
			defer repository.Close()
			clock := &manualClock{now: start}
			timers := newControlledWaitTimerFactory()
			var messages atomic.Uint64
			var receipts atomic.Uint64
			service := queue.NewService(repository,
				queue.WithClock(clock),
				queue.WithWaitTimerFactory(timers.New),
				queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("fifo-long-poll-%d", messages.Add(1)) }),
				queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(receipts.Add(1)) }),
			)
			fifo := createFIFOQueue(t, service, "long-poll.fifo", false)
			waitSeconds := 20
			result := make(chan receiveResult, 1)
			go func() {
				value, err := service.ReceiveMessageContext(context.Background(), queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, WaitTimeSeconds: &waitSeconds, ReceiveRequestAttemptID: "long-attempt"})
				result <- receiveResult{messages: value, err: err}
			}()
			request := timers.next(t)
			if request.duration != 20*time.Second {
				t.Fatalf("long poll timer = %v", request.duration)
			}
			sent := sendFIFO(t, service, fifo, "arrived", "group", "dedup")
			claimed := receiveLongPollResult(t, result)
			if claimed.err != nil || len(claimed.messages) != 1 || claimed.messages[0].ID != sent.ID {
				t.Fatalf("long poll claim = %#v, %v", claimed.messages, claimed.err)
			}
			replayed, err := service.ReceiveMessageContext(context.Background(), queue.ReceiveMessageInput{QueueName: fifo.Name, QueueID: fifo.ID, WaitTimeSeconds: &waitSeconds, ReceiveRequestAttemptID: "long-attempt"})
			if err != nil || len(replayed) != 1 || replayed[0].ReceiptHandle != claimed.messages[0].ReceiptHandle {
				t.Fatalf("long poll replay = %#v, %v", replayed, err)
			}
		})
	}
}
