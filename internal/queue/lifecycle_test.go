package queue_test

import (
	"testing"
	"time"

	"simq/internal/queue"
)

type expireRecordingRepository struct {
	queue.Repository
	command queue.ExpireCommand
	calls   int
	err     error
}

func (r *expireRecordingRepository) Expire(command queue.ExpireCommand) (int, error) {
	r.command = command
	r.calls++
	return 7, r.err
}

func TestExpireMessagesUsesExactlyOneServiceTimestamp(t *testing.T) {
	base := time.Date(2026, time.August, 23, 12, 13, 14, 987654321, time.UTC)
	testClock := &observedClock{now: base}
	repository := &expireRecordingRepository{Repository: queue.NewMemoryRepository()}
	service := queue.NewService(repository, queue.WithClock(testClock))

	expired, err := service.ExpireMessages()
	if err != nil || expired != 7 {
		t.Fatalf("ExpireMessages = %d, %v", expired, err)
	}
	if testClock.Calls() != 1 || repository.calls != 1 || !repository.command.Now.Equal(base) {
		t.Fatalf("timestamp calls = %d, repository calls = %d, command = %#v", testClock.Calls(), repository.calls, repository.command)
	}
}
