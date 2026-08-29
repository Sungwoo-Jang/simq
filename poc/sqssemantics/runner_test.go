package sqssemantics

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestExactlyOneRejectsMissingOrAmbiguousMessages(t *testing.T) {
	tests := []struct {
		name     string
		messages []Message
	}{
		{"empty", nil},
		{"many", []Message{{MessageID: "one", ReceiptHandle: "r1"}, {MessageID: "two", ReceiptHandle: "r2"}}},
		{"missing id", []Message{{ReceiptHandle: "receipt"}}},
		{"missing receipt", []Message{{MessageID: "id"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := exactlyOne(test.messages); err == nil {
				t.Fatal("exactlyOne accepted invalid observation")
			}
		})
	}
}

func TestNewRunnerRejectsUnsafeQueuePrefix(t *testing.T) {
	backend := &stubBackend{}
	for _, prefix := range []string{"Production Queue", "UPPER", strings.Repeat("a", 33)} {
		if _, err := NewRunner(backend, prefix); err == nil {
			t.Fatalf("NewRunner accepted prefix %q", prefix)
		}
	}
}

func TestAssertEmptyFailsOnObservedMessage(t *testing.T) {
	backend := &stubBackend{received: []Message{{MessageID: "id", ReceiptHandle: "receipt"}}}
	if err := assertEmpty(context.Background(), backend, Queue{}, time.Millisecond); err == nil {
		t.Fatal("assertEmpty accepted a delivered message")
	}
}

type stubBackend struct {
	received []Message
}

func (*stubBackend) Name() string { return "stub" }
func (*stubBackend) CreateQueue(context.Context, string, QueueOptions) (Queue, error) {
	return Queue{}, nil
}
func (*stubBackend) DeleteQueue(context.Context, Queue) error { return nil }
func (*stubBackend) Send(context.Context, Queue, SendInput) (SendResult, error) {
	return SendResult{}, nil
}
func (s *stubBackend) Receive(context.Context, Queue, ReceiveInput) ([]Message, error) {
	return s.received, nil
}
func (*stubBackend) Delete(context.Context, Queue, string) error { return nil }
func (*stubBackend) ChangeVisibility(context.Context, Queue, string, time.Duration) error {
	return nil
}
func (*stubBackend) ConfigureRedrive(context.Context, Queue, Queue, int) error { return nil }
