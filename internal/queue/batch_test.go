package queue_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simq/internal/queue"
	"simq/internal/storage/boltrepo"
)

func TestBatchActionsConformance(t *testing.T) {
	factories := map[string]repositoryFactory{
		"memory": func(*testing.T) queue.Repository { return queue.NewMemoryRepository() },
		"bbolt": func(t *testing.T) queue.Repository {
			directory := t.TempDir()
			_ = os.Chmod(directory, 0o700)
			repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(directory, "simq.db"), OpenTimeout: time.Second})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			return repository
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) { runBatchConformance(t, factory) })
	}
}

func runBatchConformance(t *testing.T, factory repositoryFactory) {
	repository := factory(t)
	t.Cleanup(func() { _ = repository.Close() })
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clock := &manualClock{now: now}
	var messageSequence atomic.Int64
	service := queue.NewService(repository, queue.WithClock(clock), queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("message-%02d", messageSequence.Add(1)) }))
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"VisibilityTimeout": "30"}}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	entries := make([]queue.SendMessageBatchEntry, 10)
	for index := range entries {
		entries[index] = queue.SendMessageBatchEntry{ID: fmt.Sprintf("id-%d", index), MessageBody: fmt.Sprintf("body-%d", index)}
	}
	sent, err := service.SendMessageBatch(queue.SendMessageBatchInput{QueueName: "orders", Entries: entries})
	if err != nil || len(sent.Successful) != 10 || len(sent.Failed) != 0 {
		t.Fatalf("SendMessageBatch(10) = %#v, %v", sent, err)
	}
	received, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MaxNumberOfMessages: intPointer(10)})
	if err != nil || len(received) != 10 {
		t.Fatalf("ReceiveMessage = %d, %v", len(received), err)
	}
	deletes := make([]queue.DeleteMessageBatchEntry, 10)
	changes := make([]queue.ChangeMessageVisibilityBatchEntry, 10)
	for index := range received {
		deletes[index] = queue.DeleteMessageBatchEntry{ID: fmt.Sprintf("delete-%d", index), ReceiptHandle: received[index].ReceiptHandle}
		changes[index] = queue.ChangeMessageVisibilityBatchEntry{ID: fmt.Sprintf("change-%d", index), ReceiptHandle: received[index].ReceiptHandle, VisibilityTimeout: 30}
	}
	changedTen, err := service.ChangeMessageVisibilityBatch(queue.ChangeMessageVisibilityBatchInput{QueueName: "orders", Entries: changes})
	if err != nil || len(changedTen.Successful) != 10 || len(changedTen.Failed) != 0 {
		t.Fatalf("ChangeMessageVisibilityBatch(10) = %#v, %v", changedTen, err)
	}
	deleted, err := service.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: "orders", Entries: deletes})
	if err != nil || len(deleted.Successful) != 10 || len(deleted.Failed) != 0 {
		t.Fatalf("DeleteMessageBatch = %#v, %v", deleted, err)
	}

	delay := 5
	partial, err := service.SendMessageBatch(queue.SendMessageBatchInput{QueueName: "orders", Entries: []queue.SendMessageBatchEntry{
		{ID: "good-a", MessageBody: "a"},
		{ID: "bad", MessageBody: ""},
		{ID: "good-b", MessageBody: "b", DelaySeconds: &delay, MessageAttributes: map[string]queue.MessageAttribute{"Kind": {DataType: "String.event", StringValue: "created"}}},
	}})
	if err != nil || len(partial.Successful) != 2 || partial.Successful[0].ID != "good-a" || partial.Successful[1].ID != "good-b" || len(partial.Failed) != 1 || partial.Failed[0].ID != "bad" {
		t.Fatalf("partial Send = %#v, %v", partial, err)
	}
	if partial.Successful[1].Message.AvailableAt != now.Add(5*time.Second) || partial.Successful[1].Message.MessageAttributes["Kind"].StringValue != "created" {
		t.Fatalf("stored send batch message = %#v", partial.Successful[1].Message)
	}

	allFailed, err := service.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: "orders", Entries: []queue.DeleteMessageBatchEntry{{ID: "bad-1", ReceiptHandle: "bad"}, {ID: "bad-2", ReceiptHandle: "rh_00000000000000000000000000000000"}}})
	if err != nil || len(allFailed.Successful) != 0 || len(allFailed.Failed) != 2 {
		t.Fatalf("all-failed Delete = %#v, %v", allFailed, err)
	}
	for _, failure := range allFailed.Failed {
		if !errors.Is(failure.Error, queue.ErrReceiptHandleIsInvalid) {
			t.Fatalf("Delete failure = %v", failure.Error)
		}
	}

	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "visibility"}); err != nil {
		t.Fatalf("CreateQueue visibility: %v", err)
	}
	one, err := service.SendMessageBatch(queue.SendMessageBatchInput{QueueName: "visibility", Entries: []queue.SendMessageBatchEntry{{ID: "one", MessageBody: "visibility"}}})
	if err != nil || len(one.Successful) != 1 || len(one.Failed) != 0 {
		t.Fatalf("SendMessageBatch(1): %#v, %v", one, err)
	}
	claimed, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "visibility"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim visibility message: %v, %d", err, len(claimed))
	}
	clock.Set(now.Add(time.Second))
	changed, err := service.ChangeMessageVisibilityBatch(queue.ChangeMessageVisibilityBatchInput{QueueName: "visibility", Entries: []queue.ChangeMessageVisibilityBatchEntry{{ID: "change", ReceiptHandle: claimed[0].ReceiptHandle, VisibilityTimeout: 5}, {ID: "invalid", ReceiptHandle: "bad", VisibilityTimeout: 5}}})
	if err != nil || len(changed.Successful) != 1 || len(changed.Failed) != 1 {
		t.Fatalf("Change batch = %#v, %v", changed, err)
	}
	clock.Set(now.Add(6*time.Second - time.Nanosecond))
	before, _ := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "visibility"})
	if len(before) != 0 {
		t.Fatal("visibility batch delivered early")
	}
	clock.Set(now.Add(6 * time.Second))
	at, _ := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "visibility"})
	if len(at) != 1 {
		t.Fatal("visibility batch did not deliver at exact deadline")
	}

	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "receipts", Attributes: map[string]string{"VisibilityTimeout": "0"}}); err != nil {
		t.Fatalf("Create receipts queue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "receipts", MessageBody: "receipt"}); err != nil {
		t.Fatalf("Send receipt: %v", err)
	}
	first, _ := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "receipts"})
	second, _ := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "receipts"})
	receiptDeletes, err := service.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: "receipts", Entries: []queue.DeleteMessageBatchEntry{{ID: "stale", ReceiptHandle: first[0].ReceiptHandle}, {ID: "current", ReceiptHandle: second[0].ReceiptHandle}}})
	if err != nil || len(receiptDeletes.Successful) != 2 || len(receiptDeletes.Failed) != 0 {
		t.Fatalf("stale/current Delete batch = %#v, %v", receiptDeletes, err)
	}
	consumed, err := service.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: "receipts", Entries: []queue.DeleteMessageBatchEntry{{ID: "consumed", ReceiptHandle: second[0].ReceiptHandle}}})
	if err != nil || len(consumed.Successful) != 1 || len(consumed.Failed) != 0 {
		t.Fatalf("consumed Delete batch = %#v, %v", consumed, err)
	}
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "other"}); err != nil {
		t.Fatalf("Create other queue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "other", MessageBody: "other"}); err != nil {
		t.Fatalf("Send other: %v", err)
	}
	other, _ := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "other"})
	cross, err := service.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: "receipts", Entries: []queue.DeleteMessageBatchEntry{{ID: "cross", ReceiptHandle: other[0].ReceiptHandle}}})
	if err != nil || len(cross.Failed) != 1 || !errors.Is(cross.Failed[0].Error, queue.ErrReceiptHandleIsInvalid) {
		t.Fatalf("cross-queue Delete batch = %#v, %v", cross, err)
	}

	for _, ids := range [][]string{{}, {"duplicate", "duplicate"}, {"bad.id"}} {
		request := make([]queue.DeleteMessageBatchEntry, len(ids))
		for index, id := range ids {
			request[index].ID = id
			request[index].ReceiptHandle = "bad"
		}
		if _, err := service.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: "orders", Entries: request}); err == nil {
			t.Fatalf("invalid IDs %#v accepted", ids)
		}
	}
	eleven := make([]queue.DeleteMessageBatchEntry, 11)
	for index := range eleven {
		eleven[index] = queue.DeleteMessageBatchEntry{ID: fmt.Sprintf("x%d", index), ReceiptHandle: "bad"}
	}
	if _, err := service.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: "orders", Entries: eleven}); err == nil {
		t.Fatal("11 entries accepted")
	}

	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "concurrent"}); err != nil {
		t.Fatalf("Create concurrent queue: %v", err)
	}
	var wait sync.WaitGroup
	failures := make(chan error, 20)
	for worker := 0; worker < 10; worker++ {
		worker := worker
		wait.Add(2)
		go func() {
			defer wait.Done()
			batch := make([]queue.SendMessageBatchEntry, 10)
			for index := range batch {
				batch[index] = queue.SendMessageBatchEntry{ID: fmt.Sprintf("e%d", index), MessageBody: fmt.Sprintf("batch-%d-%d", worker, index)}
			}
			result, err := service.SendMessageBatch(queue.SendMessageBatchInput{QueueName: "concurrent", Entries: batch})
			if err != nil {
				failures <- err
				return
			}
			if len(result.Successful) != 10 || len(result.Failed) != 0 {
				failures <- fmt.Errorf("batch result %#v", result)
			}
		}()
		go func() {
			defer wait.Done()
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "concurrent", MessageBody: fmt.Sprintf("single-%d", worker)}); err != nil {
				failures <- err
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("concurrent operation: %v", err)
	}
	total := 0
	for {
		messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "concurrent", MaxNumberOfMessages: intPointer(10), VisibilityTimeout: intPointer(0)})
		if err != nil {
			t.Fatalf("drain concurrent queue: %v", err)
		}
		if len(messages) == 0 {
			break
		}
		total += len(messages)
		entries := make([]queue.DeleteMessageBatchEntry, len(messages))
		for index := range messages {
			entries[index] = queue.DeleteMessageBatchEntry{ID: fmt.Sprintf("d%d", index), ReceiptHandle: messages[index].ReceiptHandle}
		}
		if _, err := service.DeleteMessageBatch(queue.DeleteMessageBatchInput{QueueName: "concurrent", Entries: entries}); err != nil {
			t.Fatalf("delete concurrent messages: %v", err)
		}
	}
	if total != 110 {
		t.Fatalf("concurrent delivered = %d, want 110", total)
	}
}

func intPointer(value int) *int { return &value }
