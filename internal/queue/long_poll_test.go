package queue_test

import (
	"context"
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

type controlledWaitTimer struct {
	channel chan time.Time
	stopped atomic.Bool
}

func (t *controlledWaitTimer) C() <-chan time.Time { return t.channel }
func (t *controlledWaitTimer) Stop() bool {
	return !t.stopped.Swap(true)
}

type timerRequest struct {
	duration time.Duration
	timer    *controlledWaitTimer
}

type controlledWaitTimerFactory struct {
	requests chan timerRequest
}

func newControlledWaitTimerFactory() *controlledWaitTimerFactory {
	return &controlledWaitTimerFactory{requests: make(chan timerRequest, 128)}
}

func (f *controlledWaitTimerFactory) New(duration time.Duration) queue.WaitTimer {
	timer := &controlledWaitTimer{channel: make(chan time.Time, 1)}
	f.requests <- timerRequest{duration: duration, timer: timer}
	return timer
}

func (f *controlledWaitTimerFactory) next(t *testing.T) timerRequest {
	t.Helper()
	select {
	case request := <-f.requests:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not create a timer")
		return timerRequest{}
	}
}

type receiveResult struct {
	messages []queue.Message
	err      error
}

func startLongPoll(ctx context.Context, service *queue.Service, queueName string, wait int) <-chan receiveResult {
	result := make(chan receiveResult, 1)
	go func() {
		messages, err := service.ReceiveMessageContext(ctx, queue.ReceiveMessageInput{QueueName: queueName, WaitTimeSeconds: &wait})
		result <- receiveResult{messages: messages, err: err}
	}()
	return result
}

func receiveLongPollResult(t *testing.T, result <-chan receiveResult) receiveResult {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not finish")
		return receiveResult{}
	}
}

func TestLongPollingConformance(t *testing.T) {
	for name, factory := range conformanceFactories() {
		t.Run(name, func(t *testing.T) {
			t.Run("immediate timeout send delay visibility retention and cancellation", func(t *testing.T) {
				repository := factory(t)
				t.Cleanup(func() { _ = repository.Close() })
				clock := &manualClock{now: time.Date(2026, time.August, 23, 14, 0, 0, 123456789, time.UTC)}
				timers := newControlledWaitTimerFactory()
				var messageID atomic.Uint64
				var receipt atomic.Uint64
				service := queue.NewService(repository,
					queue.WithClock(clock),
					queue.WithWaitTimerFactory(timers.New),
					queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("long-poll-message-%d", messageID.Add(1)) }),
					queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(receipt.Add(1)) }),
				)
				if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders", Attributes: map[string]string{"VisibilityTimeout": "5", "MessageRetentionPeriod": "60"}}); err != nil {
					t.Fatalf("CreateQueue: %v", err)
				}

				if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "immediate"}); err != nil {
					t.Fatalf("SendMessage immediate: %v", err)
				}
				wait := 20
				immediate, err := service.ReceiveMessageContext(context.Background(), queue.ReceiveMessageInput{QueueName: "orders", WaitTimeSeconds: &wait})
				if err != nil || len(immediate) != 1 || immediate[0].Body != "immediate" {
					t.Fatalf("immediate long poll = %#v, %v", immediate, err)
				}
				if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: immediate[0].ReceiptHandle}); err != nil {
					t.Fatalf("DeleteMessage immediate: %v", err)
				}

				timeoutResult := startLongPoll(context.Background(), service, "orders", wait)
				timeoutTimer := timers.next(t)
				if timeoutTimer.duration != 20*time.Second {
					t.Fatalf("poll deadline timer = %v, want 20s", timeoutTimer.duration)
				}
				clock.Advance(20 * time.Second)
				timeoutTimer.timer.channel <- clock.Now()
				timedOut := receiveLongPollResult(t, timeoutResult)
				if timedOut.err != nil || len(timedOut.messages) != 0 {
					t.Fatalf("timed out long poll = %#v, %v", timedOut.messages, timedOut.err)
				}

				sendResult := startLongPoll(context.Background(), service, "orders", wait)
				_ = timers.next(t)
				if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "sent-while-waiting"}); err != nil {
					t.Fatalf("SendMessage while waiting: %v", err)
				}
				woken := receiveLongPollResult(t, sendResult)
				if woken.err != nil || len(woken.messages) != 1 || woken.messages[0].Body != "sent-while-waiting" {
					t.Fatalf("send wake = %#v, %v", woken.messages, woken.err)
				}
				if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: woken.messages[0].ReceiptHandle}); err != nil {
					t.Fatalf("DeleteMessage: %v", err)
				}

				delay := 10
				if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "delayed", DelaySeconds: &delay}); err != nil {
					t.Fatalf("SendMessage delayed: %v", err)
				}
				delayedResult := startLongPoll(context.Background(), service, "orders", wait)
				delayedTimer := timers.next(t)
				if delayedTimer.duration != 10*time.Second {
					t.Fatalf("delay timer = %v, want 10s", delayedTimer.duration)
				}
				clock.Advance(10 * time.Second)
				delayedTimer.timer.channel <- clock.Now()
				delayed := receiveLongPollResult(t, delayedResult)
				if delayed.err != nil || len(delayed.messages) != 1 || delayed.messages[0].Body != "delayed" {
					t.Fatalf("delayed wake = %#v, %v", delayed.messages, delayed.err)
				}

				visibilityResult := startLongPoll(context.Background(), service, "orders", wait)
				visibilityTimer := timers.next(t)
				if visibilityTimer.duration != 5*time.Second {
					t.Fatalf("visibility timer = %v, want 5s", visibilityTimer.duration)
				}
				clock.Advance(5 * time.Second)
				visibilityTimer.timer.channel <- clock.Now()
				rereceived := receiveLongPollResult(t, visibilityResult)
				if rereceived.err != nil || len(rereceived.messages) != 1 || rereceived.messages[0].ReceiveCount != 2 {
					t.Fatalf("visibility wake = %#v, %v", rereceived.messages, rereceived.err)
				}
				if err := service.DeleteMessage(queue.DeleteMessageInput{QueueName: "orders", ReceiptHandle: rereceived.messages[0].ReceiptHandle}); err != nil {
					t.Fatalf("DeleteMessage rereceived: %v", err)
				}

				cancelContext, cancel := context.WithCancel(context.Background())
				cancelResult := startLongPoll(cancelContext, service, "orders", wait)
				_ = timers.next(t)
				cancel()
				cancelled := receiveLongPollResult(t, cancelResult)
				if !errors.Is(cancelled.err, context.Canceled) || len(cancelled.messages) != 0 {
					t.Fatalf("cancelled long poll = %#v, %v", cancelled.messages, cancelled.err)
				}
				if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "after-cancel"}); err != nil {
					t.Fatalf("SendMessage after cancel: %v", err)
				}
				short, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
				if err != nil || len(short) != 1 || short[0].Body != "after-cancel" {
					t.Fatalf("state after cancellation = %#v, %v", short, err)
				}
			})
		})
	}
}

type firstClaimGateRepository struct {
	queue.Repository
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *firstClaimGateRepository) Claim(input queue.ClaimInput) (queue.ClaimResult, error) {
	result, err := r.Repository.Claim(input)
	r.once.Do(func() {
		close(r.entered)
		<-r.release
	})
	return result, err
}

func TestLongPollClaimRegisterRecheckClosesLostNotificationWindow(t *testing.T) {
	base := queue.NewMemoryRepository()
	gated := &firstClaimGateRepository{Repository: base, entered: make(chan struct{}), release: make(chan struct{})}
	clock := &manualClock{now: time.Date(2026, time.August, 23, 15, 0, 0, 0, time.UTC)}
	service := queue.NewService(gated,
		queue.WithClock(clock),
		queue.WithMessageIDGenerator(func() string { return "lost-window-message" }),
		queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(1) }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	result := startLongPoll(context.Background(), service, "orders", 20)
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first claim did not enter gate")
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "between-claim-and-register"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	close(gated.release)
	woken := receiveLongPollResult(t, result)
	if woken.err != nil || len(woken.messages) != 1 || woken.messages[0].Body != "between-claim-and-register" {
		t.Fatalf("lost-window result = %#v, %v", woken.messages, woken.err)
	}
}

func TestLongPollShutdownAndMultipleWaitersDoNotDuplicateClaims(t *testing.T) {
	repository := queue.NewMemoryRepository()
	clock := &manualClock{now: time.Date(2026, time.August, 23, 16, 0, 0, 0, time.UTC)}
	timers := newControlledWaitTimerFactory()
	var receipt atomic.Uint64
	service := queue.NewService(repository,
		queue.WithClock(clock),
		queue.WithWaitTimerFactory(timers.New),
		queue.WithMessageIDGenerator(func() string { return "one-message" }),
		queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(receipt.Add(1)) }),
	)
	if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	const waiters = 10
	contexts := make([]context.CancelFunc, 0, waiters)
	results := make([]<-chan receiveResult, 0, waiters)
	for index := 0; index < waiters; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		contexts = append(contexts, cancel)
		results = append(results, startLongPoll(ctx, service, "orders", 20))
	}
	for index := 0; index < waiters; index++ {
		_ = timers.next(t)
	}
	if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "single"}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	claimed := 0
	deadline := time.After(5 * time.Second)
	for claimed == 0 {
		for index, result := range results {
			select {
			case value := <-result:
				if value.err == nil && len(value.messages) == 1 {
					claimed++
				}
				results[index] = nil
			default:
			}
		}
		select {
		case <-deadline:
			t.Fatal("no waiter claimed the sent message")
		default:
		}
	}
	for _, cancel := range contexts {
		cancel()
	}
	for _, result := range results {
		if result == nil {
			continue
		}
		value := receiveLongPollResult(t, result)
		if value.err == nil && len(value.messages) > 0 {
			claimed += len(value.messages)
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed message count = %d, want 1", claimed)
	}

	shutdownTimers := newControlledWaitTimerFactory()
	shutdownService := queue.NewService(queue.NewMemoryRepository(), queue.WithClock(clock), queue.WithWaitTimerFactory(shutdownTimers.New))
	if _, err := shutdownService.CreateQueue(queue.CreateQueueInput{QueueName: "shutdown"}); err != nil {
		t.Fatalf("CreateQueue shutdown: %v", err)
	}
	shutdownResult := startLongPoll(context.Background(), shutdownService, "shutdown", 20)
	_ = shutdownTimers.next(t)
	shutdownService.ShutdownLongPolling()
	stopped := receiveLongPollResult(t, shutdownResult)
	if !errors.Is(stopped.err, queue.ErrServiceShuttingDown) || len(stopped.messages) != 0 {
		t.Fatalf("shutdown long poll = %#v, %v", stopped.messages, stopped.err)
	}
}

func TestLongPollQueueIsolationSpuriousWakeAndRetention(t *testing.T) {
	for name, factory := range conformanceFactories() {
		t.Run(name, func(t *testing.T) {
			repository := factory(t)
			t.Cleanup(func() { _ = repository.Close() })
			clock := &manualClock{now: time.Date(2026, time.August, 23, 19, 0, 0, 0, time.UTC)}
			timers := newControlledWaitTimerFactory()
			var messageID atomic.Uint64
			var receipt atomic.Uint64
			service := queue.NewService(repository,
				queue.WithClock(clock),
				queue.WithWaitTimerFactory(timers.New),
				queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("isolation-message-%d", messageID.Add(1)) }),
				queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(receipt.Add(1)) }),
			)
			for _, queueName := range []string{"orders", "payments", "retention"} {
				if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: queueName, Attributes: map[string]string{"MessageRetentionPeriod": "60"}}); err != nil {
					t.Fatalf("CreateQueue(%s): %v", queueName, err)
				}
			}

			payments := startLongPoll(context.Background(), service, "payments", 20)
			firstPaymentTimer := timers.next(t)
			if firstPaymentTimer.duration != 20*time.Second {
				t.Fatalf("initial payment timer = %v", firstPaymentTimer.duration)
			}
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "isolated-order"}); err != nil {
				t.Fatalf("SendMessage orders: %v", err)
			}
			order, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "orders"})
			if err != nil || len(order) != 1 || order[0].Body != "isolated-order" {
				t.Fatalf("orders receive = %#v, %v", order, err)
			}
			delay := 10
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "payments", MessageBody: "delayed-payment", DelaySeconds: &delay}); err != nil {
				t.Fatalf("SendMessage payments: %v", err)
			}
			paymentTransition := timers.next(t)
			if paymentTransition.duration != 10*time.Second {
				t.Fatalf("payment transition timer = %v, want 10s", paymentTransition.duration)
			}
			clock.Advance(10 * time.Second)
			paymentTransition.timer.channel <- clock.Now()
			payment := receiveLongPollResult(t, payments)
			if payment.err != nil || len(payment.messages) != 1 || payment.messages[0].Body != "delayed-payment" {
				t.Fatalf("payment long poll = %#v, %v", payment.messages, payment.err)
			}

			longDelay := 900
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "retention", MessageBody: "expires-while-waiting", DelaySeconds: &longDelay}); err != nil {
				t.Fatalf("SendMessage retention: %v", err)
			}
			expiring := startLongPoll(context.Background(), service, "retention", 20)
			expirationTimer := timers.next(t)
			if expirationTimer.duration != 20*time.Second {
				t.Fatalf("expiration poll timer = %v, want 20s", expirationTimer.duration)
			}
			clock.Advance(60 * time.Second)
			expirationTimer.timer.channel <- clock.Now()
			expired := receiveLongPollResult(t, expiring)
			if expired.err != nil || len(expired.messages) != 0 {
				t.Fatalf("retention long poll = %#v, %v", expired.messages, expired.err)
			}
			if after, err := service.ReceiveMessage(queue.ReceiveMessageInput{QueueName: "retention"}); err != nil || len(after) != 0 {
				t.Fatalf("expired message resurrected = %#v, %v", after, err)
			}
		})
	}
}

func TestLongPollMultipleWaitersReceiveSeveralMessages(t *testing.T) {
	for name, factory := range conformanceFactories() {
		t.Run(name, func(t *testing.T) {
			repository := factory(t)
			t.Cleanup(func() { _ = repository.Close() })
			clock := &manualClock{now: time.Date(2026, time.August, 23, 20, 0, 0, 0, time.UTC)}
			timers := newControlledWaitTimerFactory()
			var messageID atomic.Uint64
			var receipt atomic.Uint64
			service := queue.NewService(repository,
				queue.WithClock(clock),
				queue.WithWaitTimerFactory(timers.New),
				queue.WithMessageIDGenerator(func() string { return fmt.Sprintf("many-message-%d", messageID.Add(1)) }),
				queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(receipt.Add(1)) }),
			)
			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
				t.Fatalf("CreateQueue: %v", err)
			}
			results := make([]<-chan receiveResult, 3)
			for index := range results {
				results[index] = startLongPoll(context.Background(), service, "orders", 20)
			}
			for range results {
				_ = timers.next(t)
			}
			for index := range results {
				if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: fmt.Sprintf("body-%d", index)}); err != nil {
					t.Fatalf("SendMessage %d: %v", index, err)
				}
			}
			bodies := make(map[string]struct{}, len(results))
			for _, result := range results {
				value := receiveLongPollResult(t, result)
				if value.err != nil || len(value.messages) != 1 {
					t.Fatalf("waiter result = %#v, %v", value.messages, value.err)
				}
				bodies[value.messages[0].Body] = struct{}{}
			}
			if len(bodies) != len(results) {
				t.Fatalf("distinct bodies = %#v, want %d", bodies, len(results))
			}
		})
	}
}

func TestLongPollMultipleWaitersClaimOneMessageOnceOnEveryRepository(t *testing.T) {
	for name, factory := range conformanceFactories() {
		t.Run(name, func(t *testing.T) {
			repository := factory(t)
			t.Cleanup(func() { _ = repository.Close() })
			clock := &manualClock{now: time.Date(2026, time.August, 23, 21, 0, 0, 0, time.UTC)}
			timers := newControlledWaitTimerFactory()
			var receipt atomic.Uint64
			service := queue.NewService(repository,
				queue.WithClock(clock),
				queue.WithWaitTimerFactory(timers.New),
				queue.WithMessageIDGenerator(func() string { return "one-conformance-message" }),
				queue.WithReceiptHandleGenerator(func() string { return deterministicReceiptHandle(receipt.Add(1)) }),
			)
			if _, err := service.CreateQueue(queue.CreateQueueInput{QueueName: "orders"}); err != nil {
				t.Fatalf("CreateQueue: %v", err)
			}

			const waiterCount = 10
			cancels := make([]context.CancelFunc, waiterCount)
			results := make([]<-chan receiveResult, waiterCount)
			for index := 0; index < waiterCount; index++ {
				ctx, cancel := context.WithCancel(context.Background())
				cancels[index] = cancel
				results[index] = startLongPoll(ctx, service, "orders", 20)
			}
			for range results {
				_ = timers.next(t)
			}
			if _, err := service.SendMessage(queue.SendMessageInput{QueueName: "orders", MessageBody: "only-once"}); err != nil {
				t.Fatalf("SendMessage: %v", err)
			}

			combined := make(chan receiveResult, waiterCount)
			for _, result := range results {
				go func() { combined <- <-result }()
			}
			first := receiveLongPollResult(t, combined)
			if first.err != nil || len(first.messages) != 1 || first.messages[0].Body != "only-once" {
				t.Fatalf("first waiter result = %#v, %v", first.messages, first.err)
			}
			for _, cancel := range cancels {
				cancel()
			}
			claimed := len(first.messages)
			for index := 1; index < waiterCount; index++ {
				value := receiveLongPollResult(t, combined)
				claimed += len(value.messages)
				if value.err != nil && !errors.Is(value.err, context.Canceled) {
					t.Fatalf("remaining waiter error = %v", value.err)
				}
			}
			if claimed != 1 {
				t.Fatalf("claimed count = %d, want 1", claimed)
			}
		})
	}
}

func conformanceFactories() map[string]repositoryFactory {
	return map[string]repositoryFactory{
		"memory": func(t *testing.T) queue.Repository { return queue.NewMemoryRepository() },
		"bbolt":  newBoltConformanceRepository,
	}
}

func newBoltConformanceRepository(t *testing.T) queue.Repository {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(directory, "simq.db"), OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return repository
}
