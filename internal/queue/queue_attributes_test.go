package queue_test

import (
	"errors"
	"testing"

	"simq/internal/queue"
)

func TestGetAndSetQueueAttributesPartialUpdateAndCreateComparison(t *testing.T) {
	service, repository, _ := newConformanceService(t, func(*testing.T) queue.Repository {
		return queue.NewMemoryRepository()
	})
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	all, err := service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: "orders", AttributeNames: []string{"All"}})
	if err != nil || all["VisibilityTimeout"] != "30" || all["DelaySeconds"] != "0" || all["MessageRetentionPeriod"] != "345600" {
		t.Fatalf("GetQueueAttributes(All) = %#v, %v", all, err)
	}
	empty, err := service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: "orders"})
	if err != nil || len(empty) != 0 {
		t.Fatalf("GetQueueAttributes(empty) = %#v, %v", empty, err)
	}

	if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{
		QueueName: "orders",
		Attributes: map[string]string{
			"VisibilityTimeout": "60",
			"DelaySeconds":      "5",
		},
	}); err != nil {
		t.Fatalf("SetQueueAttributes: %v", err)
	}
	stored, found, err := repository.Get("orders")
	if err != nil || !found || stored.VisibilityTimeout != 60 || stored.DelaySeconds != 5 || stored.MessageRetentionPeriod != queue.DefaultMessageRetentionPeriod {
		t.Fatalf("stored queue = %#v, %v, %v", stored, found, err)
	}
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{
		"VisibilityTimeout": "60", "DelaySeconds": "5", "MessageRetentionPeriod": "345600",
	}}); err != nil {
		t.Fatalf("CreateQueue with current settings: %v", err)
	}
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); !errors.Is(err, queue.ErrQueueAlreadyExists) {
		t.Fatalf("CreateQueue with old settings = %v", err)
	}
}

func TestSetQueueAttributesRejectsInvalidUpdateAtomically(t *testing.T) {
	service, repository, _ := newConformanceService(t, func(*testing.T) queue.Repository {
		return queue.NewMemoryRepository()
	})
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	for _, attributes := range []map[string]string{
		{},
		{"Unknown": "1"},
		{"VisibilityTimeout": "-1"},
		{"VisibilityTimeout": "43201"},
		{"DelaySeconds": "901"},
		{"MessageRetentionPeriod": "59"},
		{"MessageRetentionPeriod": "1209601"},
		{"VisibilityTimeout": "1", "DelaySeconds": "bad"},
	} {
		err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: "orders", Attributes: attributes})
		var invalid *queue.InvalidRequestError
		if !errors.As(err, &invalid) {
			t.Fatalf("SetQueueAttributes(%#v) = %v", attributes, err)
		}
		stored, found, getErr := repository.Get("orders")
		if getErr != nil || !found || stored.VisibilityTimeout != 30 || stored.DelaySeconds != 0 || stored.MessageRetentionPeriod != 345600 {
			t.Fatalf("invalid update partially mutated queue: %#v, %v, %v", stored, found, getErr)
		}
	}
}

func TestSetQueueAttributesAcceptsEveryInclusiveBoundary(t *testing.T) {
	service, _, _ := newConformanceService(t, func(*testing.T) queue.Repository { return queue.NewMemoryRepository() })
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	for _, attributes := range []map[string]string{
		{"VisibilityTimeout": "0", "DelaySeconds": "0", "MessageRetentionPeriod": "60"},
		{"VisibilityTimeout": "43200", "DelaySeconds": "900", "MessageRetentionPeriod": "1209600"},
	} {
		if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: "orders", Attributes: attributes}); err != nil {
			t.Fatalf("SetQueueAttributes(%#v): %v", attributes, err)
		}
	}
	if err := service.SetQueueAttributes(queue.SetQueueAttributesInput{QueueName: "missing", Attributes: map[string]string{"VisibilityTimeout": "1"}}); !errors.Is(err, queue.ErrQueueDoesNotExist) {
		t.Fatalf("missing queue error = %v", err)
	}
	for _, names := range [][]string{{"Unknown"}, {"All", "DelaySeconds"}, {"DelaySeconds", "DelaySeconds"}} {
		if _, err := service.GetQueueAttributes(queue.GetQueueAttributesInput{QueueName: "orders", AttributeNames: names}); err == nil {
			t.Fatalf("invalid selection %#v accepted", names)
		}
	}
}
