package sqssemantics

import (
	"context"
	"time"
)

type Queue struct {
	URL string
	ARN string
}

type QueueOptions struct {
	FIFO                      bool
	ContentBasedDeduplication bool
	VisibilityTimeout         time.Duration
}

type SendInput struct {
	Body            string
	GroupID         string
	DeduplicationID string
}

type SendResult struct {
	MessageID string
}

type ReceiveInput struct {
	MaxMessages       int
	VisibilityTimeout time.Duration
	WaitTime          time.Duration
}

type Message struct {
	MessageID     string
	ReceiptHandle string
	Body          string
	ReceiveCount  int
	GroupID       string
}

// Backend is the smallest worker-facing contract needed by the comparative
// scenarios. Identifiers are opaque and never compared between backends.
type Backend interface {
	Name() string
	CreateQueue(context.Context, string, QueueOptions) (Queue, error)
	DeleteQueue(context.Context, Queue) error
	Send(context.Context, Queue, SendInput) (SendResult, error)
	Receive(context.Context, Queue, ReceiveInput) ([]Message, error)
	Delete(context.Context, Queue, string) error
	ChangeVisibility(context.Context, Queue, string, time.Duration) error
	ConfigureRedrive(context.Context, Queue, Queue, int) error
}
