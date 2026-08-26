package queue_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"simq/internal/queue"
)

type observedClock struct {
	mu    sync.Mutex
	now   time.Time
	calls int
}

func (c *observedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.now
}

func (c *observedClock) Set(value time.Time) {
	c.mu.Lock()
	c.now = value
	c.calls = 0
	c.mu.Unlock()
}

func (c *observedClock) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestChangeMessageVisibilityUsesOneTimestampAndChangesOnlyDeadline(t *testing.T) {
	repository := queue.NewMemoryRepository()
	base := time.Date(2026, time.August, 23, 5, 6, 7, 123456789, time.UTC)
	testClock := &observedClock{now: base}
	service := queue.NewService(
		repository,
		queue.WithClock(testClock),
		queue.WithMessageIDGenerator(func() string { return "message-1" }),
		queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(1) }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	claimed := mustReceiveOne(t, service, "orders")

	commandTime := base.Add(5 * time.Second)
	testClock.Set(commandTime)
	if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     claimed.ReceiptHandle,
		VisibilityTimeout: 20,
	}); err != nil {
		t.Fatalf("ChangeMessageVisibility: %v", err)
	}
	if testClock.Calls() != 1 {
		t.Fatalf("clock calls = %d, want exactly 1", testClock.Calls())
	}

	stored := repository.Messages("orders")
	if len(stored) != 1 {
		t.Fatalf("stored messages = %#v", stored)
	}
	want := claimed
	want.VisibilityDeadline = commandTime.Add(20 * time.Second)
	if !reflect.DeepEqual(stored[0], want) {
		t.Fatalf("message after visibility change = %#v, want deadline-only change %#v", stored[0], want)
	}

	secondCommandTime := commandTime.Add(2 * time.Second)
	testClock.Set(secondCommandTime)
	if err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     claimed.ReceiptHandle,
		VisibilityTimeout: 3,
	}); err != nil {
		t.Fatalf("repeated ChangeMessageVisibility: %v", err)
	}
	if testClock.Calls() != 1 {
		t.Fatalf("clock calls on repeat = %d, want exactly 1", testClock.Calls())
	}
	want.VisibilityDeadline = secondCommandTime.Add(3 * time.Second)
	if got := repository.Messages("orders"); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("repeated visibility change = %#v, want %#v", got, want)
	}
}

func TestChangeMessageVisibilityRejectsInvalidInputBeforeClockOrMutation(t *testing.T) {
	repository := queue.NewMemoryRepository()
	base := time.Unix(100, 123)
	testClock := &observedClock{now: base}
	service := queue.NewService(
		repository,
		queue.WithClock(testClock),
		queue.WithMessageIDGenerator(func() string { return "message-1" }),
		queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(1) }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	claimed := mustReceiveOne(t, service, "orders")
	before := repository.Messages("orders")

	tests := []struct {
		name  string
		input queue.ChangeMessageVisibilityInput
		want  error
	}{
		{name: "invalid queue name", input: queue.ChangeMessageVisibilityInput{QueueName: "invalid.name", ReceiptHandle: claimed.ReceiptHandle, VisibilityTimeout: 1}},
		{name: "malformed handle", input: queue.ChangeMessageVisibilityInput{QueueName: "orders", ReceiptHandle: "bad", VisibilityTimeout: 1}, want: queue.ErrReceiptHandleIsInvalid},
		{name: "negative timeout", input: queue.ChangeMessageVisibilityInput{QueueName: "orders", ReceiptHandle: claimed.ReceiptHandle, VisibilityTimeout: -1}},
		{name: "timeout too large", input: queue.ChangeMessageVisibilityInput{QueueName: "orders", ReceiptHandle: claimed.ReceiptHandle, VisibilityTimeout: 43201}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testClock.Set(base.Add(time.Hour))
			err := service.ChangeMessageVisibility(test.input)
			if test.want != nil {
				if !errors.Is(err, test.want) {
					t.Fatalf("error = %v, want %v", err, test.want)
				}
			} else {
				var invalid *queue.InvalidRequestError
				if !errors.As(err, &invalid) {
					t.Fatalf("error = %v, want InvalidRequestError", err)
				}
			}
			if testClock.Calls() != 0 {
				t.Fatalf("clock calls = %d after rejected input, want 0", testClock.Calls())
			}
			if got := repository.Messages("orders"); len(got) != 1 || !reflect.DeepEqual(got[0], before[0]) {
				t.Fatalf("rejected input mutated state: %#v", got)
			}
		})
	}
}

type changeErrorRepository struct {
	queue.Repository
	err error
}

func (r changeErrorRepository) ChangeVisibility(queue.ChangeVisibilityCommand) error {
	return r.err
}

func TestChangeMessageVisibilityPropagatesRepositoryFailure(t *testing.T) {
	backing := queue.NewMemoryRepository()
	base := time.Unix(200, 456)
	setup := queue.NewService(
		backing,
		queue.WithClock(fixedClock{now: base}),
		queue.WithMessageIDGenerator(func() string { return "message-1" }),
		queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(1) }),
	)
	if _, err := setup.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := setup.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	claimed := mustReceiveOne(t, setup, "orders")
	before := backing.Messages("orders")

	sentinel := errors.New("injected change failure")
	service := queue.NewService(changeErrorRepository{Repository: backing, err: sentinel}, queue.WithClock(fixedClock{now: base.Add(time.Second)}))
	err := service.ChangeMessageVisibility(queue.ChangeMessageVisibilityInput{
		QueueName:         "orders",
		ReceiptHandle:     claimed.ReceiptHandle,
		VisibilityTimeout: 60,
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("ChangeMessageVisibility error = %v, want sentinel", err)
	}
	if got := backing.Messages("orders"); len(got) != 1 || !reflect.DeepEqual(got[0], before[0]) {
		t.Fatalf("failed visibility change mutated state: %#v", got)
	}
}
