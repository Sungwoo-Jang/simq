package queue

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrQueueAlreadyExists     = errors.New("queue already exists with different attributes")
	ErrQueueDoesNotExist      = errors.New("queue does not exist")
	ErrMessageIDExists        = errors.New("message ID already exists")
	ErrMessageIDUnavailable   = errors.New("could not allocate a unique message ID")
	ErrQueueIDUnavailable     = errors.New("could not allocate a unique queue ID")
	ErrReceiptHandleExists    = errors.New("receipt handle already exists")
	ErrReceiptUnavailable     = errors.New("could not allocate a unique receipt handle")
	ErrReceiptHandleIsInvalid = errors.New("receipt handle is invalid")
	ErrInvalidPaginationToken = errors.New("pagination token is invalid or expired")
	ErrOverLimit              = errors.New("administrative metadata limit exceeded")
	ErrQueueInUse             = errors.New("queue is in use by redrive configuration")
	ErrMoveTaskAlreadyRunning = errors.New("a message move task is already running")
	ErrMoveTaskNotRunning     = errors.New("message move task is not running")
	ErrMoveTaskDoesNotExist   = errors.New("message move task does not exist")
	ErrQueueIsNotDeadLetter   = errors.New("queue is not configured as a dead-letter queue")
	ErrRepositoryUnavailable  = errors.New("repository is unavailable")
	ErrServiceShuttingDown    = fmt.Errorf("%w: service is shutting down", ErrRepositoryUnavailable)
	ErrRepositoryClosed       = fmt.Errorf("%w: repository is closed", ErrRepositoryUnavailable)
	ErrRepositoryCorrupt      = fmt.Errorf("%w: repository is corrupt or incompatible", ErrRepositoryUnavailable)
	ErrTenantRequired         = errors.New("authenticated tenant is required")
	ErrQuotaExceeded          = errors.New("tenant storage quota exceeded")
)

type Repository interface {
	Create(Queue) (Queue, error)
	Get(string) (Queue, bool, error)
	GetByRef(QueueRef) (Queue, bool, error)
	ListQueues(ListQueuesCommand) (ListQueuesPage, error)
	DeleteQueue(QueueRef) error
	PurgeQueue(QueueRef) error
	TagQueue(QueueRef, map[string]string) error
	UntagQueue(QueueRef, []string) error
	ListQueueTags(QueueRef) (map[string]string, error)
	AddPermission(QueueRef, Permission) error
	RemovePermission(QueueRef, string) error
	ListPermissions(QueueRef) ([]Permission, error)
	SetAttributes(QueueAttributesCommand) (Queue, error)
	Enqueue(EnqueueCommand) (Message, error)
	Claim(ClaimInput) (ClaimResult, error)
	Delete(string, string, string) error
	ChangeVisibility(ChangeVisibilityCommand) error
	Expire(ExpireCommand) (int, error)
	Health() error
	Close() error
}

// RedriveRepository is the M3 extension implemented by production repositories.
// Keeping it separate preserves the small M1/M2 Repository test-double surface.
type RedriveRepository interface {
	ListDeadLetterSources(ListDeadLetterSourcesCommand) (ListDeadLetterSourcesPage, error)
	StartMoveTask(StartMoveTaskCommand) (MessageMoveTask, error)
	ListMoveTasks(QueueRef, int) ([]MessageMoveTask, error)
	CancelMoveTask(string, time.Time) (MessageMoveTask, error)
	MoveTaskStep(string, time.Time) (MoveTaskStepResult, error)
	RunningMoveTasks() ([]MessageMoveTask, error)
}

// OperationScopedRepository binds every mutation performed by one public
// request to a stable idempotency prefix. Cluster repositories implement it;
// standalone repositories intentionally do not.
type OperationScopedRepository interface {
	Repository
	ForOperation(string) Repository
	Clustered() bool
}

type ClusterRoutingRepository interface {
	Repository
	Clustered() bool
	IsLeader() bool
	LeaderAPIURL() string
}

// KeyRoutingRepository allows a repository wrapper to select the Raft group
// for an already-authorized internal tenant key before an action is decoded.
type KeyRoutingRepository interface {
	Repository
	RouteForKey(string) (clustered, leader bool, leaderURL string)
}

type ClusterMember struct{ ID, Address, Suffrage string }
type ShardStatus struct {
	ID, CatalogRevision, LeaderURL string
	Leader                         bool
}
type ClusterAdminRepository interface {
	ClusterRoutingRepository
	ClusterMembers() ([]ClusterMember, error)
	AddClusterVoter(string, string) error
	AddClusterNonvoter(string, string) error
	DemoteClusterVoter(string) error
	RemoveClusterServer(string) error
	TriggerClusterSnapshot() error
}

type ShardAdminRepository interface {
	ClusterRoutingRepository
	ShardStatuses() []ShardStatus
	ShardMembers(string) ([]ClusterMember, error)
	AddShardVoter(string, string, string) error
	AddShardNonvoter(string, string, string) error
	DemoteShardVoter(string, string) error
	RemoveShardServer(string, string) error
	TriggerShardSnapshot(string) error
}

type TenantScopedRepository interface {
	Repository
	ForTenant(string) Repository
}

type StorageQuota struct{ MaxQueues, MaxMessages, MaxPayloadBytes uint64 }
type StorageQuotaRepository interface{ SetStorageQuota(StorageQuota) }
