package queue_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"simq/internal/queue"
)

func TestCreateQueueDefaultsAndIsIdempotent(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository)

	first, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatalf("first CreateQueue: %v", err)
	}
	if first.VisibilityTimeout != 30 {
		t.Fatalf("VisibilityTimeout = %d, want 30", first.VisibilityTimeout)
	}

	second, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatalf("idempotent CreateQueue: %v", err)
	}
	if second != first {
		t.Fatalf("idempotent result = %#v, want %#v", second, first)
	}
	if repository.Count() != 1 {
		t.Fatalf("repository count = %d, want 1", repository.Count())
	}
}

func TestCreateQueueRejectsConflictingAttributesWithoutMutation(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository)

	created, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName:  "orders",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	if err != nil {
		t.Fatalf("initial CreateQueue: %v", err)
	}

	_, err = service.CreateQueue(queue.CreateQueueInput{
		QueueName:  "orders",
		Attributes: map[string]string{"VisibilityTimeout": "31"},
	})
	if !errors.Is(err, queue.ErrQueueAlreadyExists) {
		t.Fatalf("conflicting CreateQueue error = %v, want ErrQueueAlreadyExists", err)
	}

	stored, ok, err := repository.Get("orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("queue disappeared after conflicting create")
	}
	if stored != created {
		t.Fatalf("stored queue = %#v, want unchanged %#v", stored, created)
	}
	if repository.Count() != 1 {
		t.Fatalf("repository count = %d, want 1", repository.Count())
	}
}

func TestCreateQueueValidation(t *testing.T) {
	tests := []struct {
		name  string
		input queue.CreateQueueInput
	}{
		{name: "empty name", input: queue.CreateQueueInput{}},
		{name: "name too long", input: queue.CreateQueueInput{QueueName: strings.Repeat("a", 81)}},
		{name: "invalid name character", input: queue.CreateQueueInput{QueueName: "order.items"}},
		{name: "fifo name", input: queue.CreateQueueInput{QueueName: "orders.fifo"}},
		{name: "negative visibility", input: queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"VisibilityTimeout": "-1"}}},
		{name: "visibility too large", input: queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"VisibilityTimeout": "43201"}}},
		{name: "non-decimal visibility", input: queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"VisibilityTimeout": "thirty"}}},
		{name: "negative delay", input: queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"DelaySeconds": "-1"}}},
		{name: "delay too large", input: queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"DelaySeconds": "901"}}},
		{name: "retention too small", input: queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"MessageRetentionPeriod": "59"}}},
		{name: "retention too large", input: queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"MessageRetentionPeriod": "1209601"}}},
		{name: "unknown attribute", input: queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"Unknown": "1"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := queue.NewMemoryRepository()
			service := queue.NewService(repository)

			_, err := service.CreateQueue(test.input)
			var invalid *queue.InvalidRequestError
			if !errors.As(err, &invalid) {
				t.Fatalf("CreateQueue error = %v, want InvalidRequestError", err)
			}
			if repository.Count() != 0 {
				t.Fatalf("repository count = %d after rejected request, want 0", repository.Count())
			}
		})
	}
}

func TestVisibilityTimeoutBoundaries(t *testing.T) {
	for _, test := range []struct {
		value string
		want  int
	}{
		{value: "0", want: 0},
		{value: "43200", want: 43200},
	} {
		t.Run(test.value, func(t *testing.T) {
			repository := queue.NewMemoryRepository()
			service := queue.NewService(repository)

			created, err := service.CreateQueue(queue.CreateQueueInput{
				QueueName:  "orders",
				Attributes: map[string]string{"VisibilityTimeout": test.value},
			})
			if err != nil {
				t.Fatalf("CreateQueue: %v", err)
			}
			if created.VisibilityTimeout != test.want {
				t.Fatalf("VisibilityTimeout = %d, want %d", created.VisibilityTimeout, test.want)
			}
		})
	}
}

func TestCreateQueueLifecycleDefaultsAndIdempotency(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository)
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	want := queue.Queue{
		ID:                     created.ID,
		Name:                   "orders",
		LegacyURLAllowed:       true,
		VisibilityTimeout:      30,
		DelaySeconds:           0,
		MessageRetentionPeriod: 345600,
	}
	if created != want {
		t.Fatalf("created queue = %#v, want %#v", created, want)
	}
	explicit, err := service.CreateQueue(queue.CreateQueueInput{
		QueueName: "orders",
		Attributes: map[string]string{
			"VisibilityTimeout":      "30",
			"DelaySeconds":           "0",
			"MessageRetentionPeriod": "345600",
		},
	})
	if err != nil || explicit != created {
		t.Fatalf("explicit defaults = %#v, %v", explicit, err)
	}
	_, err = service.CreateQueue(queue.CreateQueueInput{
		QueueName:  "orders",
		Attributes: map[string]string{"DelaySeconds": "1"},
	})
	if !errors.Is(err, queue.ErrQueueAlreadyExists) {
		t.Fatalf("conflicting lifecycle create = %v", err)
	}
}

func TestQueueLifecycleAttributeBoundaries(t *testing.T) {
	for _, test := range []struct {
		name       string
		attributes map[string]string
	}{
		{name: "minimum", attributes: map[string]string{"DelaySeconds": "0", "MessageRetentionPeriod": "60"}},
		{name: "maximum", attributes: map[string]string{"DelaySeconds": "900", "MessageRetentionPeriod": "1209600"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := queue.NewService(queue.NewMemoryRepository())
			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders", Attributes: test.attributes}); err != nil {
				t.Fatalf("CreateQueue: %v", err)
			}
		})
	}
}

func TestMemoryRepositoryConcurrentConflictingCreateLeavesOneQueue(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository)

	const workers = 20
	var wait sync.WaitGroup
	wait.Add(workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wait.Done()
			visibility := "30"
			if i%2 == 1 {
				visibility = "31"
			}
			_, err := service.CreateQueue(queue.CreateQueueInput{
				QueueName:  "orders",
				Attributes: map[string]string{"VisibilityTimeout": visibility},
			})
			if err != nil && !errors.Is(err, queue.ErrQueueAlreadyExists) {
				t.Errorf("CreateQueue: %v", err)
			}
		}()
	}
	wait.Wait()

	if repository.Count() != 1 {
		t.Fatalf("repository count = %d, want 1", repository.Count())
	}
	stored, ok, err := repository.Get("orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("orders queue not found")
	}
	if stored.VisibilityTimeout != 30 && stored.VisibilityTimeout != 31 {
		t.Fatalf("stored VisibilityTimeout = %d, want 30 or 31", stored.VisibilityTimeout)
	}
}

func TestGetQueueReturnsExistingQueue(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository)
	created, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	got, err := service.GetQueue(queue.GetQueueInput{QueueName: "orders"})
	if err != nil {
		t.Fatalf("GetQueue: %v", err)
	}
	if got != created {
		t.Fatalf("GetQueue = %#v, want %#v", got, created)
	}
}

func TestGetQueueReturnsNotFoundWithoutMutation(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository)

	_, err := service.GetQueue(queue.GetQueueInput{QueueName: "missing"})
	if !errors.Is(err, queue.ErrQueueDoesNotExist) {
		t.Fatalf("GetQueue error = %v, want ErrQueueDoesNotExist", err)
	}
	if repository.Count() != 0 {
		t.Fatalf("repository count = %d after missing lookup, want 0", repository.Count())
	}
}

func TestGetQueueValidatesNameWithoutMutation(t *testing.T) {
	repository := queue.NewMemoryRepository()
	service := queue.NewService(repository)

	for _, name := range []string{"", "orders.invalid"} {
		_, err := service.GetQueue(queue.GetQueueInput{QueueName: name})
		var invalid *queue.InvalidRequestError
		if !errors.As(err, &invalid) {
			t.Errorf("GetQueue(%q) error = %v, want InvalidRequestError", name, err)
		}
	}
	if _, err := service.GetQueue(queue.GetQueueInput{QueueName: "orders.fifo"}); !errors.Is(err, queue.ErrQueueDoesNotExist) {
		t.Errorf("GetQueue(%q) error = %v, want ErrQueueDoesNotExist", "orders.fifo", err)
	}
	if repository.Count() != 0 {
		t.Fatalf("repository count = %d after invalid lookups, want 0", repository.Count())
	}
}
