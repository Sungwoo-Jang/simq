package queue_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"simq/internal/queue"
)

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func (c *manualClock) Set(value time.Time) {
	c.mu.Lock()
	c.now = value
	c.mu.Unlock()
}

func deterministicReceiptHandle(sequence uint64) string {
	return fmt.Sprintf("rh_%032x", sequence)
}

func newReceiveTestService(t *testing.T, visibilityTimeout string) (*queue.Service, *queue.MemoryRepository, *manualClock) {
	t.Helper()
	repository := queue.NewMemoryRepository()
	testClock := &manualClock{now: time.Date(2026, time.August, 23, 1, 2, 3, 456000000, time.UTC)}
	var receiptSequence atomic.Uint64
	service := queue.NewService(
		repository,
		queue.WithClock(testClock),
		queue.WithMessageIDGenerator(func() string { return "message-1" }),
		queue.WithReceiptHandleGenerator(func() string {
			return deterministicReceiptHandle(receiptSequence.Add(1))
		}),
	)
	_, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName:  "orders",
		Attributes: map[string]string{"VisibilityTimeout": visibilityTimeout},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	return service, repository, testClock
}

func TestReceiveMessageClaimsAtomicallyAndReturnsSystemAttributes(t *testing.T) {
	service, _, testClock := newReceiveTestService(t, "30")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "order-created"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %#v, want one", messages)
	}
	message := messages[0]
	if message.Body != "order-created" || message.ReceiveCount != 1 || message.ReceiveGeneration != 1 {
		t.Fatalf("received message = %#v", message)
	}
	if message.ReceiptHandle != deterministicReceiptHandle(1) {
		t.Fatalf("ReceiptHandle = %q", message.ReceiptHandle)
	}
	if message.FirstReceivedAtMillis != testClock.Now().UnixMilli() {
		t.Fatalf("FirstReceivedAtMillis = %d, want %d", message.FirstReceivedAtMillis, testClock.Now().UnixMilli())
	}
	if message.VisibilityDeadline != testClock.Now().Add(30*time.Second) {
		t.Fatalf("VisibilityDeadline = %v", message.VisibilityDeadline)
	}

	again, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil {
		t.Fatalf("second ReceiveMessage: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second receive returned %#v before visibility expiry", again)
	}
}

func TestReceiveMessageVisibilityBoundaryCreatesNewGeneration(t *testing.T) {
	service, _, testClock := newReceiveTestService(t, "10")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(first) != 1 {
		t.Fatalf("first ReceiveMessage = %#v, %v", first, err)
	}

	testClock.Advance(10*time.Second - time.Nanosecond)
	before, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(before) != 0 {
		t.Fatalf("receive before deadline = %#v, %v", before, err)
	}
	testClock.Advance(time.Nanosecond)
	second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(second) != 1 {
		t.Fatalf("receive at deadline = %#v, %v", second, err)
	}
	if second[0].ReceiptHandle == first[0].ReceiptHandle {
		t.Fatal("re-receive reused the receipt handle")
	}
	if second[0].ReceiveCount != 2 || second[0].ReceiveGeneration != 2 {
		t.Fatalf("second receive metadata = %#v", second[0])
	}
	if second[0].FirstReceivedAtMillis != first[0].FirstReceivedAtMillis {
		t.Fatal("first receive timestamp changed")
	}
}

func TestReceiveMessageZeroVisibilityIsImmediatelyEligible(t *testing.T) {
	service, _, _ := newReceiveTestService(t, "30")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	zero := 0
	first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", VisibilityTimeout: &zero})
	if err != nil || len(first) != 1 {
		t.Fatalf("first ReceiveMessage = %#v, %v", first, err)
	}
	second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", VisibilityTimeout: &zero})
	if err != nil || len(second) != 1 {
		t.Fatalf("second ReceiveMessage = %#v, %v", second, err)
	}
	if second[0].ReceiptHandle == first[0].ReceiptHandle || second[0].ReceiveCount != 2 {
		t.Fatalf("second receive = %#v", second[0])
	}
}

func TestTwentyConcurrentReceivesClaimOneVisibleMessageAtMostOnce(t *testing.T) {
	service, _, _ := newReceiveTestService(t, "30")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	const consumers = 20
	results := make(chan []queue.Message, consumers)
	errorsChannel := make(chan error, consumers)
	var wait sync.WaitGroup
	wait.Add(consumers)
	for i := 0; i < consumers; i++ {
		go func() {
			defer wait.Done()
			messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- messages
		}()
	}
	wait.Wait()
	close(results)
	close(errorsChannel)

	for err := range errorsChannel {
		t.Error(err)
	}
	claims := 0
	for messages := range results {
		claims += len(messages)
	}
	if claims != 1 {
		t.Fatalf("successful claims = %d, want 1", claims)
	}
}

func TestReceiveMessageBatchLimitAndQueueIsolation(t *testing.T) {
	repository := queue.NewMemoryRepository()
	var messageSequence atomic.Uint64
	var receiptSequence atomic.Uint64
	service := queue.NewService(
		repository,
		queue.WithClock(fixedClock{now: time.Unix(100, 0)}),
		queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("message-%d", messageSequence.Add(1)) }),
		queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(receiptSequence.Add(1)) }),
	)
	for _, name := range []string{"orders", "payments"} {
		if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: name}); err != nil {
			t.Fatalf("CreateQueue(%s): %v", name, err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: fmt.Sprintf("order-%d", i)}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "payments", MessageBody: "payment"}); err != nil {
		t.Fatalf("SendMessage(payment): %v", err)
	}

	max := 2
	orders, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders", MaxNumberOfMessages: &max})
	if err != nil || len(orders) != 2 {
		t.Fatalf("orders ReceiveMessage = %#v, %v", orders, err)
	}
	for _, message := range orders {
		if message.QueueName != "orders" {
			t.Fatalf("received cross-queue message %#v", message)
		}
	}
	payments, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "payments"})
	if err != nil || len(payments) != 1 || payments[0].Body != "payment" {
		t.Fatalf("payments ReceiveMessage = %#v, %v", payments, err)
	}
}

func TestReceiveMessageRejectsInvalidInputWithoutClaiming(t *testing.T) {
	service, _, _ := newReceiveTestService(t, "30")
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	for _, input := range []queue.ReceiveMessageInput{
		{QueueName: "invalid.name"},
		{QueueName: "orders", MaxNumberOfMessages: pointerTo(0)},
		{QueueName: "orders", MaxNumberOfMessages: pointerTo(11)},
		{QueueName: "orders", VisibilityTimeout: pointerTo(-1)},
		{QueueName: "orders", VisibilityTimeout: pointerTo(43201)},
	} {
		_, err := service.ReceiveMessage(input)
		var invalid *queue.InvalidRequestError
		if !errors.As(err, &invalid) {
			t.Fatalf("ReceiveMessage(%#v) error = %v, want InvalidRequestError", input, err)
		}
	}

	messages, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(messages) != 1 || messages[0].ReceiveCount != 1 {
		t.Fatalf("valid receive after invalid requests = %#v, %v", messages, err)
	}
}

func TestReceiveMessageRetriesReceiptHandleCollisionWithoutPartialClaim(t *testing.T) {
	repository := queue.NewMemoryRepository()
	firstHandle := deterministicReceiptHandle(1)
	secondHandle := deterministicReceiptHandle(2)
	generated := []string{firstHandle, firstHandle, secondHandle}
	var index atomic.Uint64
	service := queue.NewService(
		repository,
		queue.WithClock(fixedClock{now: time.Unix(100, 0)}),
		queue.WithMessageIDGenerator(func() string { return "message-1" }),
		queue.WithReceiptHandleGenerator(func() string { return generated[index.Add(1)-1] }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName:  "orders",
		Attributes: map[string]string{"VisibilityTimeout": "0"},
	}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	first, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(first) != 1 || first[0].ReceiptHandle != firstHandle {
		t.Fatalf("first ReceiveMessage = %#v, %v", first, err)
	}
	second, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
	if err != nil || len(second) != 1 || second[0].ReceiptHandle != secondHandle {
		t.Fatalf("second ReceiveMessage = %#v, %v", second, err)
	}
	if second[0].ReceiveCount != 2 || second[0].ReceiveGeneration != 2 {
		t.Fatalf("collision partially mutated receive metadata: %#v", second[0])
	}
}

func pointerTo(value int) *int {
	return &value
}
