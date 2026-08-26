package queue_test

import (
	"errors"
	"testing"
	"time"

	"simq/internal/queue"
)

func TestDeleteMessageWithCurrentHandleIsTerminalAndIdempotent(t *testing.T) {
	service, repository, _ := newReceiveTestService(t, "30")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	received, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(received) != 1 {
		t.Fatalf("ReceiveMessage = %#v, %v", received, err)
	}
	input := queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: received[0].ReceiptHandle}
	if err := service.DeleteMessage(input); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	if repository.MessageCount("orders") != 0 {
		t.Fatalf("message count = %d, want 0", repository.MessageCount("orders"))
	}
	if err := service.DeleteMessage(input); err != nil {
		t.Fatalf("repeated DeleteMessage: %v", err)
	}
	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(messages) != 0 {
		t.Fatalf("ReceiveMessage after delete = %#v, %v", messages, err)
	}
}

func TestStaleReceiptHandleCannotDeleteNewReceiveGeneration(t *testing.T) {
	service, repository, testClock := newReceiveTestService(t, "10")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(first) != 1 {
		t.Fatalf("first ReceiveMessage = %#v, %v", first, err)
	}
	testClock.Advance(10 * time.Second)
	second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(second) != 1 {
		t.Fatalf("second ReceiveMessage = %#v, %v", second, err)
	}
	if first[0].ReceiptHandle == second[0].ReceiptHandle {
		t.Fatal("receipt handle was reused")
	}

	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: first[0].ReceiptHandle}); err != nil {
		t.Fatalf("DeleteMessage(stale): %v", err)
	}
	if repository.MessageCount("orders") != 1 {
		t.Fatal("stale receipt handle deleted the newer claim")
	}
	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: second[0].ReceiptHandle}); err != nil {
		t.Fatalf("DeleteMessage(current): %v", err)
	}
	if repository.MessageCount("orders") != 0 {
		t.Fatal("current receipt handle did not delete the message")
	}
}

func TestDeleteMessageRejectsMalformedAndUnissuedHandlesWithoutMutation(t *testing.T) {
	service, repository, _ := newReceiveTestService(t, "30")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	received, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(received) != 1 {
		t.Fatalf("ReceiveMessage = %#v, %v", received, err)
	}

	for _, handle := range []string{"not-a-receipt", deterministicReceiptHandle(999)} {
		err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: handle})
		if !errors.Is(err, queue.ErrReceiptHandleIsInvalid) {
			t.Fatalf("DeleteMessage(%q) error = %v, want ErrReceiptHandleIsInvalid", handle, err)
		}
		if repository.MessageCount("orders") != 1 {
			t.Fatalf("message count changed after invalid handle %q", handle)
		}
	}

	if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: received[0].ReceiptHandle}); err != nil {
		t.Fatalf("DeleteMessage(current): %v", err)
	}
}

func TestReceiptHandleCannotOperateOnAnotherQueue(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(
		repository,
		queue.WithClock(fixedClock{now: time.Unix(100, 0)}),
		queue.WithMessageIDGenerator(func() string { return "message-1" }),
		queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(1) }),
	)
	for _, name := range []string{"orders", "payments"} {
		if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: name}); err != nil {
			t.Fatalf("CreateQueue(%s): %v", name, err)
		}
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	received, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(received) != 1 {
		t.Fatalf("ReceiveMessage = %#v, %v", received, err)
	}

	err = service.DeleteMessage(queue.DeleteMessageInput{QueueName: "payments", ReceiptHandle: received[0].ReceiptHandle})
	if !errors.Is(err, queue.ErrReceiptHandleIsInvalid) {
		t.Fatalf("cross-queue DeleteMessage error = %v, want ErrReceiptHandleIsInvalid", err)
	}
	if repository.MessageCount("orders") != 1 || repository.MessageCount("payments") != 0 {
		t.Fatal("cross-queue delete mutated queue state")
	}
}

func TestConcurrentDeleteMessageIsRaceFreeAndIdempotent(t *testing.T) {
	service, repository, _ := newReceiveTestService(t, "30")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	received, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(received) != 1 {
		t.Fatalf("ReceiveMessage = %#v, %v", received, err)
	}

	const workers = 20
	errorsChannel := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			errorsChannel <- service.DeleteMessage(queue.DeleteMessageInput{
				QueueName:     "orders",
				ReceiptHandle: received[0].ReceiptHandle,
			})
		}()
	}
	for i := 0; i < workers; i++ {
		if err := <-errorsChannel; err != nil {
			t.Errorf("DeleteMessage: %v", err)
		}
	}
	if repository.MessageCount("orders") != 0 {
		t.Fatalf("message count = %d, want 0", repository.MessageCount("orders"))
	}
}

func TestStaleDeleteRacingWithRereceiveNeverDeletesNewClaim(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		service, repository, testClock := newReceiveTestService(t, "1")
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
			t.Fatalf("iteration %d SendMessage: %v", iteration, err)
		}
		first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
		if err != nil || len(first) != 1 {
			t.Fatalf("iteration %d first ReceiveMessage = %#v, %v", iteration, first, err)
		}
		testClock.Advance(time.Second)

		start := make(chan struct{})
		receiveResult := make(chan []queue.Message, 1)
		receiveError := make(chan error, 1)
		deleteError := make(chan error, 1)
		go func() {
			<-start
			messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
			receiveResult <- messages
			receiveError <- err
		}()
		go func() {
			<-start
			deleteError <- service.DeleteMessage(queue.DeleteMessageInput{
				QueueName:     "orders",
				ReceiptHandle: first[0].ReceiptHandle,
			})
		}()
		close(start)

		messages := <-receiveResult
		if err := <-receiveError; err != nil {
			t.Fatalf("iteration %d ReceiveMessage: %v", iteration, err)
		}
		if err := <-deleteError; err != nil {
			t.Fatalf("iteration %d DeleteMessage: %v", iteration, err)
		}
		messageCount := repository.MessageCount("orders")
		switch len(messages) {
		case 0:
			if messageCount != 0 {
				t.Fatalf("iteration %d: delete won but message count = %d", iteration, messageCount)
			}
		case 1:
			if messages[0].ReceiveGeneration != 2 || messageCount != 1 {
				t.Fatalf("iteration %d: stale delete removed or corrupted new claim: %#v, count %d", iteration, messages[0], messageCount)
			}
		default:
			t.Fatalf("iteration %d: received %d messages", iteration, len(messages))
		}
	}
}
