package queue

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	simqclock "simq/internal/clock"
	"simq/internal/ownership"
	"simq/internal/topology"
)

const (
	maxVisibilityTimeout      = 43200
	maxDelaySeconds           = 900
	minMessageRetentionPeriod = 60
	maxMessageRetentionPeriod = 1209600
)

type Service struct {
	repository         Repository
	clock              simqclock.Clock
	messageIDGenerator MessageIDGenerator
	receiptGenerator   ReceiptHandleGenerator
	queueIDGenerator   QueueIDGenerator
	moveTaskGenerator  MoveTaskHandleGenerator
	waitTimerFactory   WaitTimerFactory
	waiters            *waitRegistry
	moveWorkersMu      *sync.Mutex
	moveWorkers        map[string]struct{}
	shutdown           chan struct{}
	shutdownOnce       *sync.Once
}

type MessageIDGenerator func() string
type ReceiptHandleGenerator func() string
type QueueIDGenerator func() string
type MoveTaskHandleGenerator func() string

type WaitTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type WaitTimerFactory func(time.Duration) WaitTimer

type ServiceOption func(*Service)

func WithClock(value simqclock.Clock) ServiceOption {
	return func(service *Service) {
		if value != nil {
			service.clock = value
		}
	}
}

func WithMessageIDGenerator(generator MessageIDGenerator) ServiceOption {
	return func(service *Service) {
		if generator != nil {
			service.messageIDGenerator = generator
		}
	}
}

func WithReceiptHandleGenerator(generator ReceiptHandleGenerator) ServiceOption {
	return func(service *Service) {
		if generator != nil {
			service.receiptGenerator = generator
		}
	}
}

func WithQueueIDGenerator(generator QueueIDGenerator) ServiceOption {
	return func(service *Service) {
		if generator != nil {
			service.queueIDGenerator = generator
		}
	}
}

func WithMoveTaskHandleGenerator(generator MoveTaskHandleGenerator) ServiceOption {
	return func(service *Service) {
		if generator != nil {
			service.moveTaskGenerator = generator
		}
	}
}

func WithWaitTimerFactory(factory WaitTimerFactory) ServiceOption {
	return func(service *Service) {
		if factory != nil {
			service.waitTimerFactory = factory
		}
	}
}

func NewService(repository Repository, options ...ServiceOption) *Service {
	service := &Service{
		repository:         repository,
		clock:              simqclock.System{},
		messageIDGenerator: newMessageIDGenerator(),
		receiptGenerator:   newReceiptHandleGenerator(),
		queueIDGenerator:   newQueueIDGenerator(),
		moveTaskGenerator:  newOpaqueIDGenerator("mt_"),
		waitTimerFactory:   newSystemWaitTimer,
		waiters:            newWaitRegistry(),
		moveWorkers:        make(map[string]struct{}),
		moveWorkersMu:      &sync.Mutex{},
		shutdown:           make(chan struct{}),
		shutdownOnce:       &sync.Once{},
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) Clustered() bool {
	repository, ok := s.repository.(OperationScopedRepository)
	return ok && repository.Clustered()
}

func (s *Service) ClusterRoute() (clustered, leader bool, leaderURL string) {
	repository, ok := s.repository.(ClusterRoutingRepository)
	if !ok || !repository.Clustered() {
		return false, false, ""
	}
	return true, repository.IsLeader(), repository.LeaderAPIURL()
}

func (s *Service) ClusterMembers() ([]ClusterMember, error) {
	repository, ok := s.repository.(ClusterAdminRepository)
	if !ok {
		return nil, ErrRepositoryUnavailable
	}
	return repository.ClusterMembers()
}
func (s *Service) ReconfigureCluster(action, id, address string) error {
	repository, ok := s.repository.(ClusterAdminRepository)
	if !ok {
		return ErrRepositoryUnavailable
	}
	switch action {
	case "add-voter":
		return repository.AddClusterVoter(id, address)
	case "add-nonvoter":
		return repository.AddClusterNonvoter(id, address)
	case "demote":
		return repository.DemoteClusterVoter(id)
	case "remove":
		return repository.RemoveClusterServer(id)
	default:
		return fmt.Errorf("unsupported cluster membership action")
	}
}
func (s *Service) TriggerClusterSnapshot() error {
	repository, ok := s.repository.(ClusterAdminRepository)
	if !ok {
		return ErrRepositoryUnavailable
	}
	return repository.TriggerClusterSnapshot()
}

func (s *Service) ShardStatuses() ([]ShardStatus, error) {
	repository, ok := s.repository.(ShardAdminRepository)
	if !ok {
		return nil, ErrRepositoryUnavailable
	}
	return repository.ShardStatuses(), nil
}
func (s *Service) ShardMembers(shard string) ([]ClusterMember, error) {
	repository, ok := s.repository.(ShardAdminRepository)
	if !ok {
		return nil, ErrRepositoryUnavailable
	}
	return repository.ShardMembers(shard)
}
func (s *Service) ReconfigureShard(shard, action, id, address string) error {
	repository, ok := s.repository.(ShardAdminRepository)
	if !ok {
		return ErrRepositoryUnavailable
	}
	switch action {
	case "add-voter":
		return repository.AddShardVoter(shard, id, address)
	case "add-nonvoter":
		return repository.AddShardNonvoter(shard, id, address)
	case "demote":
		return repository.DemoteShardVoter(shard, id)
	case "remove":
		return repository.RemoveShardServer(shard, id)
	default:
		return fmt.Errorf("unsupported shard membership action")
	}
}
func (s *Service) TriggerShardSnapshot(shard string) error {
	repository, ok := s.repository.(ShardAdminRepository)
	if !ok {
		return ErrRepositoryUnavailable
	}
	return repository.TriggerShardSnapshot(shard)
}

func (s *Service) BeginTenantMigration(id, digest, destination string) (ownership.Migration, error) {
	repository, ok := s.repository.(TenantMigrationAdminRepository)
	if !ok {
		return ownership.Migration{}, ErrRepositoryUnavailable
	}
	return repository.BeginTenantMigration(id, digest, destination)
}

func (s *Service) TenantMigrationStatus(id string) (ownership.Status, error) {
	repository, ok := s.repository.(TenantMigrationAdminRepository)
	if !ok {
		return ownership.Status{}, ErrRepositoryUnavailable
	}
	return repository.TenantMigrationStatus(id)
}

func (s *Service) ListTenantMigrations(limit int) ([]ownership.Migration, error) {
	repository, ok := s.repository.(TenantMigrationAdminRepository)
	if !ok {
		return nil, ErrRepositoryUnavailable
	}
	return repository.ListTenantMigrations(limit)
}

func (s *Service) AdvanceTenantMigration(id string) (ownership.Status, error) {
	repository, ok := s.repository.(TenantMigrationAdminRepository)
	if !ok {
		return ownership.Status{}, ErrRepositoryUnavailable
	}
	return repository.AdvanceTenantMigration(id)
}

func (s *Service) AbortTenantMigration(id string) (ownership.Status, error) {
	repository, ok := s.repository.(TenantMigrationAdminRepository)
	if !ok {
		return ownership.Status{}, ErrRepositoryUnavailable
	}
	return repository.AbortTenantMigration(id)
}

func (s *Service) TopologyCatalog() (topology.Catalog, error) {
	repository, ok := s.repository.(TopologyAdminRepository)
	if !ok {
		return topology.Catalog{}, ErrRepositoryUnavailable
	}
	return repository.TopologyCatalog()
}
func (s *Service) TopologyOperationStatus(id string) (topology.Status, error) {
	repository, ok := s.repository.(TopologyAdminRepository)
	if !ok {
		return topology.Status{}, ErrRepositoryUnavailable
	}
	return repository.TopologyOperationStatus(id)
}
func (s *Service) ListTopologyOperations(limit int) ([]topology.Operation, error) {
	repository, ok := s.repository.(TopologyAdminRepository)
	if !ok {
		return nil, ErrRepositoryUnavailable
	}
	return repository.ListTopologyOperations(limit)
}
func (s *Service) BeginTopologyBackfill(id string) (topology.Operation, error) {
	repository, ok := s.repository.(TopologyAdminRepository)
	if !ok {
		return topology.Operation{}, ErrRepositoryUnavailable
	}
	return repository.BeginTopologyBackfill(id)
}
func (s *Service) BeginShardActivation(id, shard string) (topology.Operation, error) {
	repository, ok := s.repository.(TopologyAdminRepository)
	if !ok {
		return topology.Operation{}, ErrRepositoryUnavailable
	}
	return repository.BeginShardActivation(id, shard)
}
func (s *Service) BeginShardDrain(id, shard string) (topology.Operation, error) {
	repository, ok := s.repository.(TopologyAdminRepository)
	if !ok {
		return topology.Operation{}, ErrRepositoryUnavailable
	}
	return repository.BeginShardDrain(id, shard)
}
func (s *Service) AdvanceTopologyOperation(id string) (topology.Status, error) {
	repository, ok := s.repository.(TopologyAdminRepository)
	if !ok {
		return topology.Status{}, ErrRepositoryUnavailable
	}
	return repository.AdvanceTopologyOperation(id)
}
func (s *Service) AbortTopologyOperation(id string) (topology.Status, error) {
	repository, ok := s.repository.(TopologyAdminRepository)
	if !ok {
		return topology.Status{}, ErrRepositoryUnavailable
	}
	return repository.AbortTopologyOperation(id)
}

// WithOperationID returns a request-scoped service sharing all process state
// while replacing only its repository view.
func (s *Service) WithOperationID(id string) *Service {
	repository, ok := s.repository.(OperationScopedRepository)
	if !ok || !repository.Clustered() {
		return s
	}
	return &Service{
		repository: repository.ForOperation(id), clock: s.clock,
		messageIDGenerator: s.messageIDGenerator, receiptGenerator: s.receiptGenerator,
		queueIDGenerator: s.queueIDGenerator, moveTaskGenerator: s.moveTaskGenerator,
		waitTimerFactory: s.waitTimerFactory, waiters: s.waiters,
		moveWorkersMu: s.moveWorkersMu, moveWorkers: s.moveWorkers,
		shutdown: s.shutdown, shutdownOnce: s.shutdownOnce,
	}
}

func (s *Service) WithTenant(tenant string) *Service {
	repository, ok := s.repository.(TenantScopedRepository)
	if !ok {
		return s
	}
	return s.cloneWithRepository(repository.ForTenant(tenant))
}

func (s *Service) cloneWithRepository(repository Repository) *Service {
	return &Service{
		repository: repository, clock: s.clock,
		messageIDGenerator: s.messageIDGenerator, receiptGenerator: s.receiptGenerator,
		queueIDGenerator: s.queueIDGenerator, moveTaskGenerator: s.moveTaskGenerator,
		waitTimerFactory: s.waitTimerFactory, waiters: s.waiters,
		moveWorkersMu: s.moveWorkersMu, moveWorkers: s.moveWorkers,
		shutdown: s.shutdown, shutdownOnce: s.shutdownOnce,
	}
}

func (s *Service) CreateQueue(input CreateQueueInput) (Queue, error) {
	if err := validateQueueName(input.QueueName); err != nil {
		return Queue{}, err
	}

	attributes, err := queueAttributes(input.Attributes)
	if err != nil {
		return Queue{}, err
	}
	if strings.HasSuffix(input.QueueName, ".fifo") != attributes.fifo {
		return Queue{}, invalidRequest("FIFO queue names and FifoQueue=true must be used together")
	}
	if _, present := input.Attributes["ContentBasedDeduplication"]; present && !attributes.fifo {
		return Queue{}, invalidRequest("ContentBasedDeduplication is valid only for FIFO queues")
	}

	for attempt := 0; attempt < 16; attempt++ {
		queueID := s.queueIDGenerator()
		if !isQueueIDWellFormed(queueID) {
			continue
		}
		created, err := s.repository.Create(Queue{
			ID:                        queueID,
			Name:                      input.QueueName,
			LegacyURLAllowed:          true,
			VisibilityTimeout:         attributes.visibilityTimeout,
			DelaySeconds:              attributes.delaySeconds,
			MessageRetentionPeriod:    attributes.messageRetentionPeriod,
			FIFO:                      attributes.fifo,
			ContentBasedDeduplication: attributes.contentBasedDeduplication,
		})
		if err == nil || !errors.Is(err, ErrQueueIDUnavailable) {
			return created, err
		}
	}
	return Queue{}, ErrQueueIDUnavailable
}

func (s *Service) GetQueueByRef(ref QueueRef) (Queue, error) {
	if err := validateQueueName(ref.Name); err != nil {
		return Queue{}, err
	}
	result, ok, err := s.repository.GetByRef(ref)
	if err != nil {
		return Queue{}, err
	}
	if !ok {
		return Queue{}, ErrQueueDoesNotExist
	}
	return result, nil
}

func (s *Service) GetQueue(input GetQueueInput) (Queue, error) {
	if err := validateQueueName(input.QueueName); err != nil {
		return Queue{}, err
	}

	result, ok, err := s.repository.Get(input.QueueName)
	if err != nil {
		return Queue{}, err
	}
	if !ok {
		return Queue{}, ErrQueueDoesNotExist
	}
	return result, nil
}

func (s *Service) SendMessage(input SendMessageInput) (Message, error) {
	if err := validateQueueRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return Message{}, err
	}
	if !utf8.ValidString(input.MessageBody) {
		return Message{}, invalidRequest("MessageBody must be valid UTF-8")
	}
	if err := validateMessageAttributes(input.MessageBody, input.MessageAttributes, true); err != nil {
		return Message{}, err
	}
	if input.DelaySeconds != nil && (*input.DelaySeconds < 0 || *input.DelaySeconds > maxDelaySeconds) {
		return Message{}, invalidRequest("DelaySeconds must be between 0 and 900")
	}
	queueValue, err := s.GetQueueByRef(QueueRef{Name: input.QueueName, ID: input.QueueID})
	if err != nil {
		return Message{}, err
	}
	if queueValue.FIFO {
		if err := validateFIFOIdentifier("MessageGroupId", input.MessageGroupID); err != nil {
			return Message{}, err
		}
		if input.MessageDeduplicationID == "" {
			if !queueValue.ContentBasedDeduplication {
				return Message{}, invalidRequest("MessageDeduplicationId is required for FIFO queues")
			}
			dedupDigest := sha256.Sum256([]byte(input.MessageBody))
			input.MessageDeduplicationID = hex.EncodeToString(dedupDigest[:])
		} else if err := validateFIFOIdentifier("MessageDeduplicationId", input.MessageDeduplicationID); err != nil {
			return Message{}, err
		}
	} else if input.MessageGroupID != "" || input.MessageDeduplicationID != "" {
		return Message{}, invalidRequest("FIFO send fields are not valid for a Standard queue")
	}
	digest := md5.Sum([]byte(input.MessageBody))
	attributeDigest, err := MessageAttributesMD5(input.MessageAttributes)
	if err != nil {
		return Message{}, err
	}
	now := s.clock.Now()
	for attempt := 0; attempt < 16; attempt++ {
		messageID := s.messageIDGenerator()
		if messageID == "" {
			continue
		}
		message := Message{
			ID:                     messageID,
			QueueName:              input.QueueName,
			Body:                   input.MessageBody,
			MD5OfBody:              hex.EncodeToString(digest[:]),
			MessageAttributes:      cloneMessageAttributes(input.MessageAttributes),
			MD5OfMessageAttributes: attributeDigest,
			MessageGroupID:         input.MessageGroupID,
			MessageDeduplicationID: input.MessageDeduplicationID,
		}
		stored, err := s.repository.Enqueue(EnqueueCommand{
			QueueName:    input.QueueName,
			QueueID:      input.QueueID,
			Now:          now,
			DelaySeconds: input.DelaySeconds,
			Message:      message,
		})
		if err == nil {
			s.waiters.notify(input.QueueName)
			return stored, nil
		}
		if !errors.Is(err, ErrMessageIDExists) {
			return Message{}, err
		}
	}
	return Message{}, ErrMessageIDUnavailable
}

func (s *Service) ReceiveMessage(input ReceiveMessageInput) ([]Message, error) {
	return s.ReceiveMessageContext(context.Background(), input)
}

func (s *Service) ReceiveMessageContext(ctx context.Context, input ReceiveMessageInput) ([]Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateQueueRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return nil, err
	}
	queueValue, err := s.GetQueueByRef(QueueRef{Name: input.QueueName, ID: input.QueueID})
	if err != nil {
		return nil, err
	}
	if input.ReceiveRequestAttemptID != "" {
		if !queueValue.FIFO {
			return nil, invalidRequest("ReceiveRequestAttemptId is valid only for FIFO queues")
		}
		if err := validateFIFOIdentifier("ReceiveRequestAttemptId", input.ReceiveRequestAttemptID); err != nil {
			return nil, err
		}
	}

	maxMessages := 1
	if input.MaxNumberOfMessages != nil {
		maxMessages = *input.MaxNumberOfMessages
	}
	if maxMessages < 1 || maxMessages > 10 {
		return nil, invalidRequest("MaxNumberOfMessages must be between 1 and 10")
	}

	var visibilityTimeout *time.Duration
	if input.VisibilityTimeout != nil {
		if *input.VisibilityTimeout < 0 || *input.VisibilityTimeout > maxVisibilityTimeout {
			return nil, invalidRequest("VisibilityTimeout must be between 0 and 43200")
		}
		value := time.Duration(*input.VisibilityTimeout) * time.Second
		visibilityTimeout = &value
	}
	allAttributes, selectedAttributes, err := messageAttributeSelection(input.MessageAttributeNames)
	if err != nil {
		return nil, err
	}
	waitSeconds := 0
	if input.WaitTimeSeconds != nil {
		waitSeconds = *input.WaitTimeSeconds
	}
	if waitSeconds < 0 || waitSeconds > 20 {
		return nil, invalidRequest("WaitTimeSeconds must be between 0 and 20")
	}

	now := s.clock.Now()
	if waitSeconds == 0 {
		result, err := s.claimMessages(input.QueueName, input.QueueID, maxMessages, visibilityTimeout, input.ReceiveRequestAttemptID, true, now)
		if err != nil {
			return nil, err
		}
		return projectReceivedMessages(result.Messages, allAttributes, selectedAttributes)
	}

	pollDeadline := now.Add(time.Duration(waitSeconds) * time.Second)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case <-s.shutdown:
			return nil, ErrServiceShuttingDown
		default:
		}

		result, err := s.claimMessages(input.QueueName, input.QueueID, maxMessages, visibilityTimeout, input.ReceiveRequestAttemptID, false, now)
		if err != nil {
			return nil, err
		}
		if len(result.Messages) > 0 || result.AttemptReplayed {
			return projectReceivedMessages(result.Messages, allAttributes, selectedAttributes)
		}
		if !now.Before(pollDeadline) {
			return s.finishReceiveAttempt(input.QueueName, input.QueueID, maxMessages, visibilityTimeout, input.ReceiveRequestAttemptID, now, allAttributes, selectedAttributes)
		}

		registration := s.waiters.subscribe(input.QueueName)
		now = s.clock.Now()
		if err := ctx.Err(); err != nil {
			registration.unsubscribe()
			return nil, err
		}
		result, err = s.claimMessages(input.QueueName, input.QueueID, maxMessages, visibilityTimeout, input.ReceiveRequestAttemptID, false, now)
		if err != nil {
			registration.unsubscribe()
			return nil, err
		}
		if len(result.Messages) > 0 || result.AttemptReplayed {
			registration.unsubscribe()
			return projectReceivedMessages(result.Messages, allAttributes, selectedAttributes)
		}
		if !now.Before(pollDeadline) {
			registration.unsubscribe()
			return s.finishReceiveAttempt(input.QueueName, input.QueueID, maxMessages, visibilityTimeout, input.ReceiveRequestAttemptID, now, allAttributes, selectedAttributes)
		}

		wakeAt := pollDeadline
		if !result.NextTransition.IsZero() && result.NextTransition.Before(wakeAt) {
			wakeAt = result.NextTransition
		}
		duration := wakeAt.Sub(now)
		if duration < 0 {
			duration = 0
		}
		timer := s.waitTimerFactory(duration)
		select {
		case <-registration.channel:
		case <-timer.C():
		case <-ctx.Done():
			registration.unsubscribe()
			timer.Stop()
			return nil, ctx.Err()
		case <-s.shutdown:
			registration.unsubscribe()
			timer.Stop()
			return nil, ErrServiceShuttingDown
		}
		registration.unsubscribe()
		timer.Stop()
		now = s.clock.Now()
	}
}

func (s *Service) finishReceiveAttempt(queueName, queueID string, maxMessages int, visibilityTimeout *time.Duration, attemptID string, now time.Time, allAttributes bool, selectedAttributes map[string]struct{}) ([]Message, error) {
	if attemptID == "" {
		return []Message{}, nil
	}
	result, err := s.claimMessages(queueName, queueID, maxMessages, visibilityTimeout, attemptID, true, now)
	if err != nil {
		return nil, err
	}
	return projectReceivedMessages(result.Messages, allAttributes, selectedAttributes)
}

func (s *Service) claimMessages(queueName, queueID string, maxMessages int, visibilityTimeout *time.Duration, attemptID string, storeEmptyAttempt bool, now time.Time) (ClaimResult, error) {
	for attempt := 0; attempt < 16; attempt++ {
		handles := make([]string, maxMessages)
		validHandles := true
		for index := range handles {
			handles[index] = s.receiptGenerator()
			if !isReceiptHandleWellFormed(handles[index]) {
				validHandles = false
			}
		}
		if !validHandles {
			continue
		}
		result, err := s.repository.Claim(ClaimInput{
			QueueName:               queueName,
			QueueID:                 queueID,
			Now:                     now,
			MaxNumberOfMessages:     maxMessages,
			VisibilityTimeout:       visibilityTimeout,
			ReceiptHandles:          handles,
			ReceiveRequestAttemptID: attemptID,
			StoreEmptyAttempt:       storeEmptyAttempt,
		})
		if err == nil {
			for _, target := range result.MovedTo {
				s.waiters.notify(target.Name)
			}
			return result, nil
		}
		if !errors.Is(err, ErrReceiptHandleExists) && !errors.Is(err, ErrReceiptUnavailable) {
			return ClaimResult{}, err
		}
	}
	return ClaimResult{}, ErrReceiptUnavailable
}

func projectReceivedMessages(messages []Message, allAttributes bool, selectedAttributes map[string]struct{}) ([]Message, error) {
	projected := make([]Message, len(messages))
	for index := range messages {
		var err error
		projected[index], err = projectMessageAttributes(messages[index], allAttributes, selectedAttributes)
		if err != nil {
			return nil, err
		}
	}
	return projected, nil
}

func (s *Service) GetQueueAttributes(input GetQueueAttributesInput) (map[string]string, error) {
	if err := validateQueueRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return nil, err
	}
	all, selected, err := queueAttributeSelection(input.AttributeNames)
	if err != nil {
		return nil, err
	}
	queueValue, ok, err := s.repository.GetByRef(QueueRef{Name: input.QueueName, ID: input.QueueID})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrQueueDoesNotExist
	}
	values := map[string]string{
		"VisibilityTimeout":      strconv.Itoa(queueValue.VisibilityTimeout),
		"DelaySeconds":           strconv.Itoa(queueValue.DelaySeconds),
		"MessageRetentionPeriod": strconv.Itoa(queueValue.MessageRetentionPeriod),
		"QueueArn":               QueueARN(QueueRef{Name: queueValue.Name, ID: queueValue.ID}),
		"FifoQueue":              strconv.FormatBool(queueValue.FIFO),
	}
	if queueValue.FIFO {
		values["ContentBasedDeduplication"] = strconv.FormatBool(queueValue.ContentBasedDeduplication)
	}
	redrivePolicy, err := redrivePolicyJSON(queueValue.RedrivePolicy)
	if err != nil {
		return nil, err
	}
	values["RedrivePolicy"] = redrivePolicy
	permissions, err := s.repository.ListPermissions(QueueRef{Name: input.QueueName, ID: input.QueueID})
	if err != nil {
		return nil, err
	}
	policy, err := permissionPolicyJSON(permissions)
	if err != nil {
		return nil, err
	}
	values["Policy"] = policy
	result := make(map[string]string)
	for name, value := range values {
		if _, wanted := selected[name]; all || wanted {
			result[name] = value
		}
	}
	return result, nil
}

func (s *Service) SetQueueAttributes(input SetQueueAttributesInput) error {
	if err := validateQueueRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return err
	}
	if len(input.Attributes) == 0 {
		return invalidRequest("Attributes must contain at least one entry")
	}
	command := QueueAttributesCommand{QueueName: input.QueueName, QueueID: input.QueueID}
	for name, value := range input.Attributes {
		switch name {
		case "VisibilityTimeout":
			parsed, err := decimalQueueAttribute(name, value, 0, maxVisibilityTimeout)
			if err != nil {
				return err
			}
			command.VisibilityTimeout = &parsed
		case "DelaySeconds":
			parsed, err := decimalQueueAttribute(name, value, 0, maxDelaySeconds)
			if err != nil {
				return err
			}
			command.DelaySeconds = &parsed
		case "MessageRetentionPeriod":
			parsed, err := decimalQueueAttribute(name, value, minMessageRetentionPeriod, maxMessageRetentionPeriod)
			if err != nil {
				return err
			}
			command.MessageRetentionPeriod = &parsed
		case "RedrivePolicy":
			policy, err := parseRedrivePolicy(value)
			if err != nil {
				return err
			}
			command.RedrivePolicySet = true
			command.RedrivePolicy = policy
		default:
			return invalidRequest("unsupported queue attribute %q", name)
		}
	}
	_, err := s.repository.SetAttributes(command)
	return err
}

func (s *Service) DeleteMessage(input DeleteMessageInput) error {
	if err := validateQueueRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return err
	}
	if !isReceiptHandleWellFormed(input.ReceiptHandle) {
		return ErrReceiptHandleIsInvalid
	}
	return s.repository.Delete(input.QueueName, input.QueueID, input.ReceiptHandle)
}

func (s *Service) ChangeMessageVisibility(input ChangeMessageVisibilityInput) error {
	if err := validateQueueRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return err
	}
	if !isReceiptHandleWellFormed(input.ReceiptHandle) {
		return ErrReceiptHandleIsInvalid
	}
	if input.VisibilityTimeout < 0 || input.VisibilityTimeout > maxVisibilityTimeout {
		return invalidRequest("VisibilityTimeout must be between 0 and 43200")
	}

	now := s.clock.Now()
	err := s.repository.ChangeVisibility(ChangeVisibilityCommand{
		QueueName:         input.QueueName,
		QueueID:           input.QueueID,
		ReceiptHandle:     input.ReceiptHandle,
		Now:               now,
		VisibilityTimeout: time.Duration(input.VisibilityTimeout) * time.Second,
	})
	if err == nil {
		s.waiters.notify(input.QueueName)
	}
	return err
}

func (s *Service) SendMessageBatch(input SendMessageBatchInput) (SendMessageBatchResult, error) {
	ids := make([]string, len(input.Entries))
	total := 0
	for index, entry := range input.Entries {
		ids[index] = entry.ID
		total += MessageLogicalSize(entry.MessageBody, entry.MessageAttributes)
	}
	if err := validateBatchRequest(ids); err != nil {
		return SendMessageBatchResult{}, err
	}
	if total > MaxMessageBytes {
		return SendMessageBatchResult{}, &BatchRequestError{Code: "BatchRequestTooLong", Message: "The batch message payload is too large."}
	}
	if _, err := s.GetQueueByRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return SendMessageBatchResult{}, err
	}
	result := SendMessageBatchResult{Successful: []SendMessageBatchSuccess{}, Failed: []BatchFailure{}}
	for _, entry := range input.Entries {
		message, err := s.SendMessage(SendMessageInput{QueueName: input.QueueName, QueueID: input.QueueID, MessageBody: entry.MessageBody, DelaySeconds: entry.DelaySeconds, MessageAttributes: entry.MessageAttributes, MessageGroupID: entry.MessageGroupID, MessageDeduplicationID: entry.MessageDeduplicationID})
		if err != nil {
			result.Failed = append(result.Failed, BatchFailure{ID: entry.ID, Error: err})
			continue
		}
		result.Successful = append(result.Successful, SendMessageBatchSuccess{ID: entry.ID, Message: message})
	}
	return result, nil
}

func (s *Service) DeleteMessageBatch(input DeleteMessageBatchInput) (BatchResult, error) {
	ids := make([]string, len(input.Entries))
	for index := range input.Entries {
		ids[index] = input.Entries[index].ID
	}
	if err := validateBatchRequest(ids); err != nil {
		return BatchResult{}, err
	}
	if _, err := s.GetQueueByRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return BatchResult{}, err
	}
	result := BatchResult{Successful: []string{}, Failed: []BatchFailure{}}
	for _, entry := range input.Entries {
		err := s.DeleteMessage(DeleteMessageInput{QueueName: input.QueueName, QueueID: input.QueueID, ReceiptHandle: entry.ReceiptHandle})
		if err != nil {
			result.Failed = append(result.Failed, BatchFailure{ID: entry.ID, Error: err})
			continue
		}
		result.Successful = append(result.Successful, entry.ID)
	}
	return result, nil
}

func (s *Service) ChangeMessageVisibilityBatch(input ChangeMessageVisibilityBatchInput) (BatchResult, error) {
	ids := make([]string, len(input.Entries))
	for index := range input.Entries {
		ids[index] = input.Entries[index].ID
	}
	if err := validateBatchRequest(ids); err != nil {
		return BatchResult{}, err
	}
	if _, err := s.GetQueueByRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return BatchResult{}, err
	}
	result := BatchResult{Successful: []string{}, Failed: []BatchFailure{}}
	for _, entry := range input.Entries {
		err := s.ChangeMessageVisibility(ChangeMessageVisibilityInput{QueueName: input.QueueName, QueueID: input.QueueID, ReceiptHandle: entry.ReceiptHandle, VisibilityTimeout: entry.VisibilityTimeout})
		if err != nil {
			result.Failed = append(result.Failed, BatchFailure{ID: entry.ID, Error: err})
			continue
		}
		result.Successful = append(result.Successful, entry.ID)
	}
	return result, nil
}

type paginationToken struct {
	Version  int    `json:"v"`
	Prefix   string `json:"p"`
	After    string `json:"a"`
	Revision uint64 `json:"r"`
}

func (s *Service) ListQueues(input ListQueuesInput) (ListQueuesPage, string, error) {
	if err := validateQueuePrefix(input.QueueNamePrefix); err != nil {
		return ListQueuesPage{}, "", err
	}
	limit := 1000
	if input.MaxResults != nil {
		limit = *input.MaxResults
		if limit < 1 || limit > 1000 {
			return ListQueuesPage{}, "", invalidRequest("MaxResults must be between 1 and 1000")
		}
	}
	command := ListQueuesCommand{Prefix: input.QueueNamePrefix, Limit: limit}
	if input.NextToken != "" {
		token, err := decodePaginationToken(input.NextToken)
		if err != nil || token.Prefix != input.QueueNamePrefix {
			return ListQueuesPage{}, "", ErrInvalidPaginationToken
		}
		command.After = token.After
		command.ExpectedRevision = &token.Revision
	}
	page, err := s.repository.ListQueues(command)
	if err != nil {
		return ListQueuesPage{}, "", err
	}
	next := ""
	if input.MaxResults != nil && page.HasMore && len(page.Queues) > 0 {
		next, err = encodePaginationToken(paginationToken{Version: 1, Prefix: input.QueueNamePrefix, After: page.Queues[len(page.Queues)-1].Name, Revision: page.NamespaceRevision})
		if err != nil {
			return ListQueuesPage{}, "", err
		}
	}
	return page, next, nil
}

func encodePaginationToken(value paginationToken) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodePaginationToken(value string) (paginationToken, error) {
	if len(value) > 1024 {
		return paginationToken{}, ErrInvalidPaginationToken
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return paginationToken{}, err
	}
	var token paginationToken
	decoder := json.NewDecoder(strings.NewReader(string(decoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&token); err != nil || token.Version != 1 || token.After == "" {
		return paginationToken{}, ErrInvalidPaginationToken
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return paginationToken{}, ErrInvalidPaginationToken
	}
	return token, nil
}

func (s *Service) DeleteQueue(ref QueueRef) error {
	if err := validateQueueRef(ref); err != nil {
		return err
	}
	err := s.repository.DeleteQueue(ref)
	if err == nil {
		s.waiters.notify(ref.Name)
	}
	return err
}

func (s *Service) PurgeQueue(ref QueueRef) error {
	if err := validateQueueRef(ref); err != nil {
		return err
	}
	return s.repository.PurgeQueue(ref)
}

func (s *Service) TagQueue(input TagQueueInput) error {
	if err := validateQueueRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return err
	}
	if len(input.Tags) == 0 {
		return invalidRequest("Tags must contain at least one entry")
	}
	for key, value := range input.Tags {
		if !utf8.ValidString(key) || len(key) < 1 || len(key) > 128 || strings.HasPrefix(strings.ToLower(key), "simq:") {
			return invalidRequest("tag keys must be 1 to 128 UTF-8 bytes and must not use the simq: prefix")
		}
		if !utf8.ValidString(value) || len(value) > 256 {
			return invalidRequest("tag values must be at most 256 UTF-8 bytes")
		}
	}
	return s.repository.TagQueue(QueueRef{Name: input.QueueName, ID: input.QueueID}, input.Tags)
}

func (s *Service) UntagQueue(input UntagQueueInput) error {
	if err := validateQueueRef(QueueRef{Name: input.QueueName, ID: input.QueueID}); err != nil {
		return err
	}
	if len(input.TagKeys) < 1 || len(input.TagKeys) > 50 {
		return invalidRequest("TagKeys must contain between 1 and 50 entries")
	}
	seen := make(map[string]struct{}, len(input.TagKeys))
	for _, key := range input.TagKeys {
		if !utf8.ValidString(key) || len(key) < 1 || len(key) > 128 || strings.HasPrefix(strings.ToLower(key), "simq:") {
			return invalidRequest("tag keys must be 1 to 128 UTF-8 bytes and must not use the simq: prefix")
		}
		if _, duplicate := seen[key]; duplicate {
			return invalidRequest("TagKeys entries must be unique")
		}
		seen[key] = struct{}{}
	}
	return s.repository.UntagQueue(QueueRef{Name: input.QueueName, ID: input.QueueID}, input.TagKeys)
}

func (s *Service) ListQueueTags(ref QueueRef) (map[string]string, error) {
	if err := validateQueueRef(ref); err != nil {
		return nil, err
	}
	return s.repository.ListQueueTags(ref)
}

func (s *Service) AddPermission(input AddPermissionInput) error {
	ref := QueueRef{Name: input.QueueName, ID: input.QueueID}
	if err := validateQueueRef(ref); err != nil {
		return err
	}
	if err := validatePermissionLabel(input.Label); err != nil {
		return err
	}
	if len(input.AWSAccountIDs) < 1 || len(input.AWSAccountIDs) > 10 {
		return invalidRequest("AWSAccountIds must contain between 1 and 10 entries")
	}
	if len(input.Actions) < 1 || len(input.Actions) > 7 {
		return invalidRequest("Actions must contain between 1 and 7 entries")
	}
	accounts := append([]string(nil), input.AWSAccountIDs...)
	actions := append([]string(nil), input.Actions...)
	if err := validateDistinct(accounts, func(value string) bool { return len(value) == 12 && allDecimal(value) }, "AWSAccountIds"); err != nil {
		return err
	}
	if err := validateDistinct(actions, validPermissionAction, "Actions"); err != nil {
		return err
	}
	sort.Strings(accounts)
	sort.Strings(actions)
	return s.repository.AddPermission(ref, Permission{Label: input.Label, AWSAccountIDs: accounts, Actions: actions})
}

func (s *Service) RemovePermission(input RemovePermissionInput) error {
	ref := QueueRef{Name: input.QueueName, ID: input.QueueID}
	if err := validateQueueRef(ref); err != nil {
		return err
	}
	if err := validatePermissionLabel(input.Label); err != nil {
		return err
	}
	return s.repository.RemovePermission(ref, input.Label)
}

func validatePermissionLabel(label string) error {
	if len(label) < 1 || len(label) > 80 {
		return invalidRequest("Label must be between 1 and 80 characters")
	}
	for _, character := range label {
		if !isQueueNameCharacter(character) {
			return invalidRequest("Label contains an invalid character")
		}
	}
	return nil
}

func ValidatePermissionMetadata(permission Permission) error {
	if err := validatePermissionLabel(permission.Label); err != nil {
		return err
	}
	if len(permission.AWSAccountIDs) < 1 || len(permission.AWSAccountIDs) > 10 || len(permission.Actions) < 1 || len(permission.Actions) > 7 {
		return invalidRequest("permission statement has invalid list limits")
	}
	if err := validateDistinct(permission.AWSAccountIDs, func(value string) bool { return len(value) == 12 && allDecimal(value) }, "AWSAccountIds"); err != nil {
		return err
	}
	if err := validateDistinct(permission.Actions, validPermissionAction, "Actions"); err != nil {
		return err
	}
	if !sort.StringsAreSorted(permission.AWSAccountIDs) || !sort.StringsAreSorted(permission.Actions) {
		return invalidRequest("permission statement lists must be sorted")
	}
	return nil
}

func validateDistinct(values []string, valid func(string) bool, field string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !valid(value) {
			return invalidRequest("%s contains an invalid entry", field)
		}
		if _, duplicate := seen[value]; duplicate {
			return invalidRequest("%s entries must be unique", field)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func allDecimal(value string) bool {
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

var permissionActions = map[string]struct{}{
	"AddPermission": {}, "ChangeMessageVisibility": {}, "ChangeMessageVisibilityBatch": {}, "CreateQueue": {}, "DeleteMessage": {}, "DeleteMessageBatch": {}, "DeleteQueue": {}, "GetQueueAttributes": {}, "GetQueueUrl": {}, "ListDeadLetterSourceQueues": {}, "ListMessageMoveTasks": {}, "ListQueueTags": {}, "ListQueues": {}, "PurgeQueue": {}, "ReceiveMessage": {}, "RemovePermission": {}, "SendMessage": {}, "SendMessageBatch": {}, "SetQueueAttributes": {}, "StartMessageMoveTask": {}, "CancelMessageMoveTask": {}, "TagQueue": {}, "UntagQueue": {},
}

func validPermissionAction(value string) bool {
	if value == "*" {
		return true
	}
	_, ok := permissionActions[value]
	return ok
}

func permissionPolicyJSON(permissions []Permission) (string, error) {
	type document struct {
		Version   string       `json:"Version"`
		Statement []Permission `json:"Statement"`
	}
	if permissions == nil {
		permissions = []Permission{}
	}
	encoded, err := json.Marshal(document{Version: "SimQ-2026-08-24", Statement: permissions})
	return string(encoded), err
}

func validateQueueRef(ref QueueRef) error {
	if err := validateQueueName(ref.Name); err != nil {
		return err
	}
	if ref.ID != "" && !isQueueIDWellFormed(ref.ID) {
		return invalidRequest("QueueUrl contains an invalid QueueId")
	}
	return nil
}

func validateQueuePrefix(prefix string) error {
	if len(prefix) > 80 {
		return invalidRequest("QueueNamePrefix must not exceed 80 characters")
	}
	for _, character := range prefix {
		if !isQueueNameCharacter(character) {
			return invalidRequest("QueueNamePrefix contains an invalid character")
		}
	}
	return nil
}

func validateBatchRequest(ids []string) error {
	if len(ids) == 0 {
		return &BatchRequestError{Code: "EmptyBatchRequest", Message: "The batch request must contain at least one entry."}
	}
	if len(ids) > 10 {
		return &BatchRequestError{Code: "TooManyEntriesInBatchRequest", Message: "The batch request must contain at most 10 entries."}
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if len(id) < 1 || len(id) > 80 {
			return &BatchRequestError{Code: "InvalidBatchEntryId", Message: "A batch entry ID is invalid."}
		}
		for _, character := range id {
			if !isQueueNameCharacter(character) {
				return &BatchRequestError{Code: "InvalidBatchEntryId", Message: "A batch entry ID is invalid."}
			}
		}
		if _, exists := seen[id]; exists {
			return &BatchRequestError{Code: "BatchEntryIdsNotDistinct", Message: "Batch entry IDs must be unique."}
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (s *Service) ShutdownLongPolling() {
	s.shutdownOnce.Do(func() { close(s.shutdown) })
}

func (s *Service) Health() error {
	return s.repository.Health()
}

func (s *Service) ExpireMessages() (int, error) {
	return s.repository.Expire(ExpireCommand{Now: s.clock.Now()})
}

type redrivePaginationToken struct {
	Version  int      `json:"v"`
	Target   QueueRef `json:"t"`
	After    string   `json:"a"`
	Revision uint64   `json:"r"`
}

func (s *Service) redriveRepository() (RedriveRepository, error) {
	repository, ok := s.repository.(RedriveRepository)
	if !ok {
		return nil, ErrRepositoryUnavailable
	}
	return repository, nil
}

func (s *Service) ListDeadLetterSourceQueues(input ListDeadLetterSourcesInput) (ListDeadLetterSourcesPage, string, error) {
	if err := validateQueueRef(input.Target); err != nil || input.Target.ID == "" {
		return ListDeadLetterSourcesPage{}, "", invalidRequest("QueueUrl must identify a queue generation")
	}
	limit := 1000
	if input.MaxResults != nil {
		limit = *input.MaxResults
		if limit < 1 || limit > 1000 {
			return ListDeadLetterSourcesPage{}, "", invalidRequest("MaxResults must be between 1 and 1000")
		}
	}
	command := ListDeadLetterSourcesCommand{Target: input.Target, Limit: limit}
	if input.NextToken != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(input.NextToken)
		if err != nil || len(decoded) > 1024 {
			return ListDeadLetterSourcesPage{}, "", ErrInvalidPaginationToken
		}
		var token redrivePaginationToken
		decoder := json.NewDecoder(strings.NewReader(string(decoded)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&token); err != nil || token.Version != 1 || token.After == "" || token.Target != input.Target {
			return ListDeadLetterSourcesPage{}, "", ErrInvalidPaginationToken
		}
		command.After = token.After
		command.ExpectedRevision = &token.Revision
	}
	repository, err := s.redriveRepository()
	if err != nil {
		return ListDeadLetterSourcesPage{}, "", err
	}
	page, err := repository.ListDeadLetterSources(command)
	if err != nil {
		return ListDeadLetterSourcesPage{}, "", err
	}
	next := ""
	if input.MaxResults != nil && page.HasMore && len(page.Queues) > 0 {
		encoded, err := json.Marshal(redrivePaginationToken{Version: 1, Target: input.Target, After: page.Queues[len(page.Queues)-1].Name, Revision: page.RedriveRevision})
		if err != nil {
			return ListDeadLetterSourcesPage{}, "", err
		}
		next = base64.RawURLEncoding.EncodeToString(encoded)
	}
	return page, next, nil
}

func (s *Service) StartMessageMoveTask(input StartMessageMoveTaskInput) (MessageMoveTask, error) {
	if err := validateQueueRef(input.Source); err != nil || input.Source.ID == "" {
		return MessageMoveTask{}, invalidRequest("SourceArn must identify a queue generation")
	}
	if input.Destination != nil {
		if err := validateQueueRef(*input.Destination); err != nil || input.Destination.ID == "" || *input.Destination == input.Source {
			return MessageMoveTask{}, invalidRequest("DestinationArn must identify a different queue generation")
		}
	}
	rate := 500
	if input.MaxMessagesPerSecond != nil {
		rate = *input.MaxMessagesPerSecond
		if rate < 1 || rate > 500 {
			return MessageMoveTask{}, invalidRequest("MaxNumberOfMessagesPerSecond must be between 1 and 500")
		}
	}
	repository, err := s.redriveRepository()
	if err != nil {
		return MessageMoveTask{}, err
	}
	for attempt := 0; attempt < 16; attempt++ {
		handle := s.moveTaskGenerator()
		if len(handle) < 4 || !strings.HasPrefix(handle, "mt_") {
			continue
		}
		task, err := repository.StartMoveTask(StartMoveTaskCommand{Handle: handle, Source: input.Source, Destination: input.Destination, MaxMessagesPerSecond: rate, Now: s.clock.Now()})
		if err == nil {
			s.startMoveWorker(task.Handle)
			return task, nil
		}
		if !errors.Is(err, ErrMessageIDExists) {
			return MessageMoveTask{}, err
		}
	}
	return MessageMoveTask{}, ErrMessageIDUnavailable
}

func (s *Service) ListMessageMoveTasks(input ListMessageMoveTasksInput) ([]MessageMoveTask, error) {
	if err := validateQueueRef(input.Source); err != nil || input.Source.ID == "" {
		return nil, invalidRequest("SourceArn must identify a queue generation")
	}
	limit := 1
	if input.MaxResults != nil {
		limit = *input.MaxResults
		if limit < 1 || limit > 10 {
			return nil, invalidRequest("MaxResults must be between 1 and 10")
		}
	}
	repository, err := s.redriveRepository()
	if err != nil {
		return nil, err
	}
	return repository.ListMoveTasks(input.Source, limit)
}

func (s *Service) CancelMessageMoveTask(handle string) (MessageMoveTask, error) {
	if len(handle) < 4 || !strings.HasPrefix(handle, "mt_") {
		return MessageMoveTask{}, invalidRequest("TaskHandle is invalid")
	}
	repository, err := s.redriveRepository()
	if err != nil {
		return MessageMoveTask{}, err
	}
	return repository.CancelMoveTask(handle, s.clock.Now())
}

func (s *Service) ResumeMessageMoveTasks() error {
	repository, err := s.redriveRepository()
	if err != nil {
		return err
	}
	tasks, err := repository.RunningMoveTasks()
	if err != nil {
		return err
	}
	for _, task := range tasks {
		s.startMoveWorker(task.Handle)
	}
	return nil
}

func (s *Service) startMoveWorker(handle string) {
	s.moveWorkersMu.Lock()
	if _, running := s.moveWorkers[handle]; running {
		s.moveWorkersMu.Unlock()
		return
	}
	s.moveWorkers[handle] = struct{}{}
	s.moveWorkersMu.Unlock()
	go func() {
		defer func() {
			s.moveWorkersMu.Lock()
			delete(s.moveWorkers, handle)
			s.moveWorkersMu.Unlock()
		}()
		repository, err := s.redriveRepository()
		if err != nil {
			return
		}
		for {
			select {
			case <-s.shutdown:
				return
			default:
			}
			result, err := repository.MoveTaskStep(handle, s.clock.Now())
			if err != nil {
				timer := s.waitTimerFactory(10 * time.Millisecond)
				select {
				case <-timer.C():
				case <-s.shutdown:
					timer.Stop()
					return
				}
				timer.Stop()
				continue
			}
			if result.Terminal {
				return
			}
			if result.Destination != nil {
				s.waiters.notify(result.Destination.Name)
			}
			delay := time.Second / time.Duration(result.Task.MaxNumberOfMessagesPerSecond)
			timer := s.waitTimerFactory(delay)
			select {
			case <-timer.C():
			case <-s.shutdown:
				timer.Stop()
				return
			}
			timer.Stop()
		}
	}()
}

func isReceiptHandleWellFormed(handle string) bool {
	if len(handle) != 35 || !strings.HasPrefix(handle, "rh_") {
		return false
	}
	for _, character := range handle[3:] {
		if character >= '0' && character <= '9' ||
			character >= 'a' && character <= 'f' ||
			character >= 'A' && character <= 'F' {
			continue
		}
		return false
	}
	return true
}

func newMessageIDGenerator() MessageIDGenerator {
	return newOpaqueIDGenerator("")
}

func newReceiptHandleGenerator() ReceiptHandleGenerator {
	return newOpaqueIDGenerator("rh_")
}

func newQueueIDGenerator() QueueIDGenerator { return newOpaqueIDGenerator("q_") }

func isQueueIDWellFormed(value string) bool {
	if len(value) != 34 || !strings.HasPrefix(value, "q_") {
		return false
	}
	for _, character := range value[2:] {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func newOpaqueIDGenerator(label string) func() string {
	prefixBytes := make([]byte, 8)
	if _, err := rand.Read(prefixBytes); err != nil {
		clear(prefixBytes)
	}
	prefix := hex.EncodeToString(prefixBytes)
	var sequence atomic.Uint64
	return func() string {
		return fmt.Sprintf("%s%s%016x", label, prefix, sequence.Add(1))
	}
}

func validateQueueName(name string) error {
	if len(name) == 0 {
		return invalidRequest("QueueName is required")
	}
	if len(name) > 80 {
		return invalidRequest("QueueName must not exceed 80 characters")
	}
	base := strings.TrimSuffix(name, ".fifo")
	if base == "" {
		return invalidRequest("QueueName contains an invalid character")
	}
	for _, character := range base {
		if !isQueueNameCharacter(character) {
			return invalidRequest("QueueName contains an invalid character")
		}
	}
	return nil
}

func isQueueNameCharacter(character rune) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' ||
		character == '-' || character == '_'
}

type effectiveQueueAttributes struct {
	visibilityTimeout         int
	delaySeconds              int
	messageRetentionPeriod    int
	fifo                      bool
	contentBasedDeduplication bool
}

func queueAttributes(attributes map[string]string) (effectiveQueueAttributes, error) {
	result := effectiveQueueAttributes{
		visibilityTimeout:      DefaultVisibilityTimeout,
		delaySeconds:           DefaultDelaySeconds,
		messageRetentionPeriod: DefaultMessageRetentionPeriod,
	}
	for name, value := range attributes {
		switch name {
		case "VisibilityTimeout":
			parsed, err := decimalQueueAttribute(name, value, 0, maxVisibilityTimeout)
			if err != nil {
				return effectiveQueueAttributes{}, err
			}
			result.visibilityTimeout = parsed
		case "DelaySeconds":
			parsed, err := decimalQueueAttribute(name, value, 0, maxDelaySeconds)
			if err != nil {
				return effectiveQueueAttributes{}, err
			}
			result.delaySeconds = parsed
		case "MessageRetentionPeriod":
			parsed, err := decimalQueueAttribute(name, value, minMessageRetentionPeriod, maxMessageRetentionPeriod)
			if err != nil {
				return effectiveQueueAttributes{}, err
			}
			result.messageRetentionPeriod = parsed
		case "FifoQueue":
			if value != "true" && value != "false" {
				return effectiveQueueAttributes{}, invalidRequest("FifoQueue must be true or false")
			}
			result.fifo = value == "true"
		case "ContentBasedDeduplication":
			if value != "true" && value != "false" {
				return effectiveQueueAttributes{}, invalidRequest("ContentBasedDeduplication must be true or false")
			}
			result.contentBasedDeduplication = value == "true"
		default:
			return effectiveQueueAttributes{}, invalidRequest("unsupported queue attribute %q", name)
		}
	}
	return result, nil
}

func validateFIFOIdentifier(name, value string) error {
	if len(value) < 1 || len(value) > 128 || !utf8.ValidString(value) {
		return invalidRequest("%s must be between 1 and 128 UTF-8 bytes", name)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return invalidRequest("%s contains a control character", name)
		}
	}
	return nil
}

func decimalQueueAttribute(name, value string, minimum, maximum int) (int, error) {
	if value == "" {
		return 0, invalidRequest("%s must be a decimal string", name)
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, invalidRequest("%s must be a decimal string", name)
		}
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, invalidRequest("%s must be between %d and %d", name, minimum, maximum)
	}
	return parsed, nil
}
