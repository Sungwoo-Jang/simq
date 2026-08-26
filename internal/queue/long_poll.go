package queue

import (
	"sync"
	"time"
)

type systemWaitTimer struct {
	timer *time.Timer
}

func newSystemWaitTimer(duration time.Duration) WaitTimer {
	return &systemWaitTimer{timer: time.NewTimer(duration)}
}

func (t *systemWaitTimer) C() <-chan time.Time { return t.timer.C }
func (t *systemWaitTimer) Stop() bool          { return t.timer.Stop() }

type waitState struct {
	channel chan struct{}
	count   int
}

type waitRegistry struct {
	mu     sync.Mutex
	queues map[string]*waitState
}

type waitRegistration struct {
	registry *waitRegistry
	queue    string
	state    *waitState
	channel  <-chan struct{}
	once     sync.Once
}

func newWaitRegistry() *waitRegistry {
	return &waitRegistry{queues: make(map[string]*waitState)}
}

func (r *waitRegistry) subscribe(queueName string) *waitRegistration {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.queues[queueName]
	if state == nil {
		state = &waitState{channel: make(chan struct{})}
		r.queues[queueName] = state
	}
	state.count++
	return &waitRegistration{registry: r, queue: queueName, state: state, channel: state.channel}
}

func (r *waitRegistry) notify(queueName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.queues[queueName]
	if state == nil {
		return
	}
	delete(r.queues, queueName)
	close(state.channel)
}

func (r *waitRegistration) unsubscribe() {
	r.once.Do(func() {
		r.registry.mu.Lock()
		defer r.registry.mu.Unlock()
		if r.registry.queues[r.queue] != r.state {
			return
		}
		r.state.count--
		if r.state.count == 0 {
			delete(r.registry.queues, r.queue)
		}
	})
}
