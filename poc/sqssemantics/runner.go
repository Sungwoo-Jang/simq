package sqssemantics

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	shortVisibility = 2 * time.Second
	pollWait        = 1 * time.Second
	scenarioLimit   = 90 * time.Second
)

type ScenarioResult struct {
	Name           string `json:"name"`
	Status         string `json:"status"`
	DurationMillis int64  `json:"durationMillis"`
	Error          string `json:"error,omitempty"`
}

type Report struct {
	Backend        string           `json:"backend"`
	StartedAt      time.Time        `json:"startedAt"`
	DurationMillis int64            `json:"durationMillis"`
	Status         string           `json:"status"`
	Scenarios      []ScenarioResult `json:"scenarios"`
	CleanupErrors  int              `json:"cleanupErrors"`
}

type Runner struct {
	backend Backend
	prefix  string
	now     func() time.Time
}

func NewRunner(backend Backend, prefix string) (*Runner, error) {
	if backend == nil {
		return nil, errors.New("backend is required")
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "simq-poc"
	}
	if len(prefix) > 32 {
		return nil, errors.New("queue prefix must be at most 32 characters")
	}
	for _, character := range prefix {
		if character != '-' && character != '_' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return nil, errors.New("queue prefix may contain lowercase letters, digits, hyphen, and underscore")
		}
	}
	return &Runner{backend: backend, prefix: prefix, now: time.Now}, nil
}

func (r *Runner) Run(ctx context.Context) (Report, error) {
	started := r.now().UTC()
	report := Report{Backend: r.backend.Name(), StartedAt: started, Status: "passed"}
	scenarios := []struct {
		name string
		run  func(context.Context, *queueScope) error
	}{
		{"acknowledged-roundtrip", r.acknowledgedRoundtrip},
		{"worker-crash-redelivery", r.workerCrashRedelivery},
		{"visibility-extension", r.visibilityExtension},
		{"fifo-group-ordering", r.fifoGroupOrdering},
		{"fifo-deduplication", r.fifoDeduplication},
		{"dead-letter-redrive", r.deadLetterRedrive},
	}

	var failures []error
	for _, scenario := range scenarios {
		scenarioStarted := r.now()
		scenarioCtx, cancel := context.WithTimeout(ctx, scenarioLimit)
		scope := &queueScope{backend: r.backend}
		err := scenario.run(scenarioCtx, scope)
		cancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		cleanupErrors := scope.cleanup(cleanupCtx)
		cleanupCancel()
		report.CleanupErrors += cleanupErrors
		result := ScenarioResult{Name: scenario.name, Status: "passed", DurationMillis: time.Since(scenarioStarted).Milliseconds()}
		if err != nil || cleanupErrors != 0 {
			result.Status = "failed"
			if err != nil {
				result.Error = err.Error()
				failures = append(failures, fmt.Errorf("%s: %w", scenario.name, err))
			}
			if cleanupErrors != 0 {
				if result.Error == "" {
					result.Error = "queue cleanup failed"
				}
				failures = append(failures, fmt.Errorf("%s: %d cleanup errors", scenario.name, cleanupErrors))
			}
		}
		report.Scenarios = append(report.Scenarios, result)
		if ctx.Err() != nil {
			break
		}
	}
	report.DurationMillis = time.Since(started).Milliseconds()
	if len(failures) != 0 {
		report.Status = "failed"
	}
	return report, errors.Join(failures...)
}

type queueScope struct {
	backend Backend
	queues  []Queue
}

func (s *queueScope) create(ctx context.Context, name string, options QueueOptions) (Queue, error) {
	queue, err := s.backend.CreateQueue(ctx, name, options)
	if err == nil {
		s.queues = append(s.queues, queue)
	}
	return queue, err
}

func (s *queueScope) cleanup(ctx context.Context) int {
	errors := 0
	for index := len(s.queues) - 1; index >= 0; index-- {
		if err := s.backend.DeleteQueue(ctx, s.queues[index]); err != nil {
			errors++
		}
	}
	return errors
}

func (r *Runner) queueName(scenario string, fifo bool) (string, error) {
	random := make([]byte, 4)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate queue suffix: %w", err)
	}
	name := fmt.Sprintf("%s-%s-%s-%s", r.prefix, r.now().UTC().Format("20060102t150405"), scenario, hex.EncodeToString(random))
	if fifo {
		name += ".fifo"
	}
	return name, nil
}

func (r *Runner) create(ctx context.Context, scope *queueScope, scenario string, options QueueOptions) (Queue, error) {
	name, err := r.queueName(scenario, options.FIFO)
	if err != nil {
		return Queue{}, err
	}
	queue, err := scope.create(ctx, name, options)
	if err != nil {
		return Queue{}, fmt.Errorf("create disposable queue: %w", err)
	}
	return queue, nil
}

func (r *Runner) acknowledgedRoundtrip(ctx context.Context, scope *queueScope) error {
	queue, err := r.create(ctx, scope, "ack", QueueOptions{VisibilityTimeout: shortVisibility})
	if err != nil {
		return err
	}
	sent, err := r.backend.Send(ctx, queue, SendInput{Body: "acknowledged-roundtrip"})
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	received, err := receiveEventually(ctx, r.backend, queue, 15*time.Second)
	if err != nil {
		return err
	}
	message, err := exactlyOne(received)
	if err != nil {
		return err
	}
	if message.MessageID != sent.MessageID || message.Body != "acknowledged-roundtrip" {
		return errors.New("received message did not match acknowledged send")
	}
	if err := r.backend.Delete(ctx, queue, message.ReceiptHandle); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return assertEmpty(ctx, r.backend, queue, shortVisibility+2*time.Second)
}

func (r *Runner) workerCrashRedelivery(ctx context.Context, scope *queueScope) error {
	queue, err := r.create(ctx, scope, "crash", QueueOptions{VisibilityTimeout: shortVisibility})
	if err != nil {
		return err
	}
	sent, err := r.backend.Send(ctx, queue, SendInput{Body: "worker-crash"})
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	firstBatch, err := receiveEventually(ctx, r.backend, queue, 15*time.Second)
	if err != nil {
		return err
	}
	first, err := exactlyOne(firstBatch)
	if err != nil {
		return err
	}
	if err := assertEmpty(ctx, r.backend, queue, time.Second); err != nil {
		return fmt.Errorf("message was not hidden after first receive: %w", err)
	}
	secondBatch, err := receiveEventually(ctx, r.backend, queue, shortVisibility+15*time.Second)
	if err != nil {
		return fmt.Errorf("redelivery: %w", err)
	}
	second, err := exactlyOne(secondBatch)
	if err != nil {
		return err
	}
	if second.MessageID != sent.MessageID || second.Body != first.Body {
		return errors.New("redelivery changed logical message")
	}
	if second.ReceiptHandle == first.ReceiptHandle {
		return errors.New("redelivery reused the stale receipt handle")
	}
	if second.ReceiveCount < 2 {
		return fmt.Errorf("redelivery receive count = %d, want at least 2", second.ReceiveCount)
	}
	return r.backend.Delete(ctx, queue, second.ReceiptHandle)
}

func (r *Runner) visibilityExtension(ctx context.Context, scope *queueScope) error {
	queue, err := r.create(ctx, scope, "visibility", QueueOptions{VisibilityTimeout: shortVisibility})
	if err != nil {
		return err
	}
	if _, err := r.backend.Send(ctx, queue, SendInput{Body: "visibility-extension"}); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	batch, err := receiveEventually(ctx, r.backend, queue, 15*time.Second)
	if err != nil {
		return err
	}
	message, err := exactlyOne(batch)
	if err != nil {
		return err
	}
	extension := 4 * time.Second
	if err := r.backend.ChangeVisibility(ctx, queue, message.ReceiptHandle, extension); err != nil {
		return fmt.Errorf("extend visibility: %w", err)
	}
	if err := assertEmpty(ctx, r.backend, queue, 3*time.Second); err != nil {
		return fmt.Errorf("message reappeared before extended deadline: %w", err)
	}
	redelivered, err := receiveEventually(ctx, r.backend, queue, extension+15*time.Second)
	if err != nil {
		return fmt.Errorf("receive after extension: %w", err)
	}
	next, err := exactlyOne(redelivered)
	if err != nil {
		return err
	}
	return r.backend.Delete(ctx, queue, next.ReceiptHandle)
}

func (r *Runner) fifoGroupOrdering(ctx context.Context, scope *queueScope) error {
	queue, err := r.create(ctx, scope, "order", QueueOptions{FIFO: true, VisibilityTimeout: 10 * time.Second})
	if err != nil {
		return err
	}
	inputs := []SendInput{
		{Body: "a-1", GroupID: "a", DeduplicationID: "a-1"},
		{Body: "a-2", GroupID: "a", DeduplicationID: "a-2"},
	}
	for _, input := range inputs {
		if _, err := r.backend.Send(ctx, queue, input); err != nil {
			return fmt.Errorf("send %s: %w", input.Body, err)
		}
	}
	firstBatch, err := receiveEventually(ctx, r.backend, queue, 15*time.Second)
	if err != nil {
		return err
	}
	first, err := exactlyOne(firstBatch)
	if err != nil {
		return err
	}
	if first.Body != "a-1" || first.GroupID != "a" {
		return fmt.Errorf("first FIFO message = %q group %q, want a-1 group a", first.Body, first.GroupID)
	}
	if _, err := r.backend.Send(ctx, queue, SendInput{Body: "b-1", GroupID: "b", DeduplicationID: "b-1"}); err != nil {
		return fmt.Errorf("send b-1 while group a is in flight: %w", err)
	}
	otherBatch, err := receiveEventually(ctx, r.backend, queue, 15*time.Second)
	if err != nil {
		return fmt.Errorf("independent group did not progress: %w", err)
	}
	other, err := exactlyOne(otherBatch)
	if err != nil {
		return err
	}
	if other.Body != "b-1" || other.GroupID != "b" {
		return fmt.Errorf("in-flight group head failed to block a-2; received %q group %q", other.Body, other.GroupID)
	}
	if err := r.backend.Delete(ctx, queue, other.ReceiptHandle); err != nil {
		return fmt.Errorf("delete independent group: %w", err)
	}
	if err := r.backend.Delete(ctx, queue, first.ReceiptHandle); err != nil {
		return fmt.Errorf("delete group head: %w", err)
	}
	lastBatch, err := receiveEventually(ctx, r.backend, queue, 15*time.Second)
	if err != nil {
		return err
	}
	last, err := exactlyOne(lastBatch)
	if err != nil {
		return err
	}
	if last.Body != "a-2" || last.GroupID != "a" {
		return fmt.Errorf("unblocked FIFO message = %q group %q, want a-2 group a", last.Body, last.GroupID)
	}
	return r.backend.Delete(ctx, queue, last.ReceiptHandle)
}

func (r *Runner) fifoDeduplication(ctx context.Context, scope *queueScope) error {
	queue, err := r.create(ctx, scope, "dedup", QueueOptions{FIFO: true, VisibilityTimeout: shortVisibility})
	if err != nil {
		return err
	}
	input := SendInput{Body: "deduplicated", GroupID: "group", DeduplicationID: "one-logical-send"}
	first, err := r.backend.Send(ctx, queue, input)
	if err != nil {
		return fmt.Errorf("first send: %w", err)
	}
	second, err := r.backend.Send(ctx, queue, input)
	if err != nil {
		return fmt.Errorf("duplicate send: %w", err)
	}
	if first.MessageID == "" || second.MessageID == "" {
		return errors.New("deduplicated send omitted acknowledgement")
	}
	batch, err := receiveEventually(ctx, r.backend, queue, 15*time.Second)
	if err != nil {
		return err
	}
	message, err := exactlyOne(batch)
	if err != nil {
		return err
	}
	if err := r.backend.Delete(ctx, queue, message.ReceiptHandle); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return assertEmpty(ctx, r.backend, queue, shortVisibility+2*time.Second)
}

func (r *Runner) deadLetterRedrive(ctx context.Context, scope *queueScope) error {
	dlq, err := r.create(ctx, scope, "dlq", QueueOptions{VisibilityTimeout: shortVisibility})
	if err != nil {
		return err
	}
	source, err := r.create(ctx, scope, "source", QueueOptions{VisibilityTimeout: shortVisibility})
	if err != nil {
		return err
	}
	if err := r.backend.ConfigureRedrive(ctx, source, dlq, 2); err != nil {
		return fmt.Errorf("configure redrive: %w", err)
	}
	if _, err := r.backend.Send(ctx, source, SendInput{Body: "poison"}); err != nil {
		return fmt.Errorf("send poison message: %w", err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		batch, err := receiveEventually(ctx, r.backend, source, 30*time.Second)
		if err != nil {
			return fmt.Errorf("source receive %d: %w", attempt, err)
		}
		message, err := exactlyOne(batch)
		if err != nil {
			return err
		}
		if message.Body != "poison" || message.ReceiveCount < attempt {
			return fmt.Errorf("source receive %d returned unexpected message/count", attempt)
		}
		if err := r.backend.ChangeVisibility(ctx, source, message.ReceiptHandle, 0); err != nil {
			return fmt.Errorf("release poison message %d: %w", attempt, err)
		}
	}
	// SimQ transfers atomically while scanning the source at the next receive
	// boundary. Amazon SQS may already have moved the message asynchronously.
	// In either case a third source delivery would violate maxReceiveCount=2.
	sourceAfterThreshold, err := r.backend.Receive(ctx, source, ReceiveInput{MaxMessages: 1, WaitTime: pollWait})
	if err != nil {
		return fmt.Errorf("check source redrive boundary: %w", err)
	}
	if len(sourceAfterThreshold) != 0 {
		return errors.New("source delivered the message after its dead-letter receive threshold")
	}
	transferred, err := receiveEventually(ctx, r.backend, dlq, 45*time.Second)
	if err != nil {
		return fmt.Errorf("receive from DLQ: %w", err)
	}
	message, err := exactlyOne(transferred)
	if err != nil {
		return err
	}
	if message.Body != "poison" {
		return fmt.Errorf("DLQ body = %q, want poison", message.Body)
	}
	return r.backend.Delete(ctx, dlq, message.ReceiptHandle)
}

func receiveEventually(ctx context.Context, backend Backend, queue Queue, limit time.Duration) ([]Message, error) {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		messages, err := backend.Receive(ctx, queue, ReceiveInput{MaxMessages: 1, WaitTime: pollWait})
		if err != nil {
			return nil, fmt.Errorf("receive: %w", err)
		}
		if len(messages) != 0 {
			return messages, nil
		}
		if err := sleepContext(ctx, 200*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("timed out waiting for a message")
}

func assertEmpty(ctx context.Context, backend Backend, queue Queue, duration time.Duration) error {
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		wait := pollWait
		if remaining < wait {
			wait = remaining
		}
		messages, err := backend.Receive(ctx, queue, ReceiveInput{MaxMessages: 1, WaitTime: wait})
		if err != nil {
			return fmt.Errorf("empty check receive: %w", err)
		}
		if len(messages) != 0 {
			return fmt.Errorf("received %d unexpected messages", len(messages))
		}
	}
	return nil
}

func exactlyOne(messages []Message) (Message, error) {
	if len(messages) != 1 {
		return Message{}, fmt.Errorf("received %d messages, want exactly 1", len(messages))
	}
	message := messages[0]
	if message.MessageID == "" || message.ReceiptHandle == "" {
		return Message{}, errors.New("received message omitted ID or receipt handle")
	}
	return message, nil
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
