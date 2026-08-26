package queue_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	simqclock "simq/internal/clock"
	"simq/internal/queue"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

var _ simqclock.Clock = fixedClock{}

func TestSendMessageStoresImmutableEnvelope(t *testing.T) {
	repository := queue.NewMemoryRepository()
	sentAt := time.Date(2026, time.August, 23, 1, 2, 3, 456000000, time.UTC)
	service := queue.NewService(
		repository,
		queue.WithClock(fixedClock{now: sentAt}),
		queue.WithMessageIDGenerator(func() string { return "message-1" }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	message, err := service.SendMessage(queue.SendMessageInput{
		QueueName:   "orders",
		MessageBody: "order-created",
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if message.ID != "message-1" {
		t.Fatalf("MessageId = %q, want message-1", message.ID)
	}
	if message.MD5OfBody != "bb493e6546e1863734c792e8ea97e3ba" {
		t.Fatalf("MD5OfBody = %q", message.MD5OfBody)
	}
	if message.SentAtMillis != sentAt.UnixMilli() {
		t.Fatalf("SentAtMillis = %d, want %d", message.SentAtMillis, sentAt.UnixMilli())
	}
	if !message.AvailableAt.Equal(sentAt) {
		t.Fatalf("AvailableAt = %v, want %v", message.AvailableAt, sentAt)
	}
	if !message.ExpiresAt.Equal(sentAt.Add(96 * time.Hour)) {
		t.Fatalf("ExpiresAt = %v, want %v", message.ExpiresAt, sentAt.Add(96*time.Hour))
	}

	stored := repository.Messages("orders")
	if len(stored) != 1 || !reflect.DeepEqual(stored[0], message) {
		t.Fatalf("stored messages = %#v, want %#v", stored, message)
	}
}

func TestSendMessageResolvesQueueAndPerMessageDelay(t *testing.T) {
	repository := queue.NewMemoryRepository()
	now := time.Date(2026, time.August, 23, 1, 2, 3, 456789123, time.UTC)
	service := queue.NewService(repository, queue.WithClock(fixedClock{now: now}))
	if _, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName: "orders",
		Attributes: map[string]string{
			"DelaySeconds":           "10",
			"MessageRetentionPeriod": "60",
		},
	}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	queueDefault, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "default"})
	if err != nil {
		t.Fatalf("default SendMessage: %v", err)
	}
	override := 3
	perMessage, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "override", DelaySeconds: &override})
	if err != nil {
		t.Fatalf("override SendMessage: %v", err)
	}
	if !queueDefault.AvailableAt.Equal(now.Add(10*time.Second)) || !perMessage.AvailableAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("availability defaults = %v, %v", queueDefault.AvailableAt, perMessage.AvailableAt)
	}
	if !queueDefault.ExpiresAt.Equal(now.Add(60*time.Second)) || !perMessage.ExpiresAt.Equal(queueDefault.ExpiresAt) {
		t.Fatalf("retention deadlines = %v, %v", queueDefault.ExpiresAt, perMessage.ExpiresAt)
	}
}

func TestSendMessageRejectsDelayWithoutMutation(t *testing.T) {
	for _, delay := range []int{-1, 901} {
		repository := queue.NewMemoryRepository()
		service := queue.NewService(repository)
		if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
			t.Fatalf("CreateQueue: %v", err)
		}
		_, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body", DelaySeconds: &delay})
		var invalid *queue.InvalidRequestError
		if !errors.As(err, &invalid) {
			t.Fatalf("delay %d error = %v, want InvalidRequestError", delay, err)
		}
		if repository.MessageCount("orders") != 0 {
			t.Fatalf("delay %d mutated repository", delay)
		}
	}
}

func TestSendMessageValidatesBodyWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "over one MiB", body: strings.Repeat("a", (1<<20)+1)},
		{name: "invalid UTF-8", body: string([]byte{0xff})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := queue.NewMemoryRepository()
			service := queue.NewService(repository)
			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
				t.Fatalf("CreateQueue: %v", err)
			}

			_, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: test.body})
			var invalid *queue.InvalidRequestError
			if !errors.As(err, &invalid) {
				t.Fatalf("SendMessage error = %v, want InvalidRequestError", err)
			}
			if repository.MessageCount("orders") != 0 {
				t.Fatalf("message count = %d after invalid send, want 0", repository.MessageCount("orders"))
			}
		})
	}
}

func TestSendMessageRequiresExistingQueue(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository)

	_, err := service.SendMessage(queue.SendMessageInput{QueueName: "missing", MessageBody: "body"})
	if !errors.Is(err, queue.ErrQueueDoesNotExist) {
		t.Fatalf("SendMessage error = %v, want ErrQueueDoesNotExist", err)
	}
	if repository.MessageCount("missing") != 0 {
		t.Fatalf("message count = %d after missing queue send, want 0", repository.MessageCount("missing"))
	}
}

func TestConcurrentSendMessageIDsAreUnique(t *testing.T) {
	repository := queue.NewMemoryRepository()
	var sequence atomic.Uint64
	service := queue.NewService(
		repository,
		queue.WithMessageIDGenerator(func() string {
			return fmt.Sprintf("message-%d", sequence.Add(1))
		}),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	const workers = 20
	results := make(chan queue.Message, workers)
	errorsChannel := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			message, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "body"})
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- message
		}()
	}

	seen := make(map[string]struct{}, workers)
	for i := 0; i < workers; i++ {
		select {
		case err := <-errorsChannel:
			t.Fatal(err)
		case message := <-results:
			if _, exists := seen[message.ID]; exists {
				t.Fatalf("duplicate MessageId %q", message.ID)
			}
			seen[message.ID] = struct{}{}
		}
	}
	if repository.MessageCount("orders") != workers {
		t.Fatalf("message count = %d, want %d", repository.MessageCount("orders"), workers)
	}
}

func TestSendMessageRetriesMessageIDCollision(t *testing.T) {
	repository := queue.NewMemoryRepository()
	generated := []string{"duplicate", "duplicate", "unique"}
	var index atomic.Uint64
	service := queue.NewService(
		repository,
		queue.WithMessageIDGenerator(func() string {
			return generated[index.Add(1)-1]
		}),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	first, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "first"})
	if err != nil {
		t.Fatalf("first SendMessage: %v", err)
	}
	second, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "second"})
	if err != nil {
		t.Fatalf("second SendMessage: %v", err)
	}
	if first.ID != "duplicate" || second.ID != "unique" {
		t.Fatalf("MessageIds = %q, %q; want duplicate, unique", first.ID, second.ID)
	}
	if repository.MessageCount("orders") != 2 {
		t.Fatalf("message count = %d, want 2", repository.MessageCount("orders"))
	}
}

func TestSendMessageFailsWhenUniqueIDCannotBeAllocated(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(
		repository,
		queue.WithMessageIDGenerator(func() string { return "duplicate" }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "first"}); err != nil {
		t.Fatalf("first SendMessage: %v", err)
	}

	_, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "second"})
	if !errors.Is(err, queue.ErrMessageIDUnavailable) {
		t.Fatalf("second SendMessage error = %v, want ErrMessageIDUnavailable", err)
	}
	if repository.MessageCount("orders") != 1 {
		t.Fatalf("message count = %d, want 1", repository.MessageCount("orders"))
	}
}
