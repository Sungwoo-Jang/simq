package queue

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type receiptBinding struct {
	QueueName  string
	QueueID    string
	MessageID  string
	Generation uint64
}

type fifoDedupRecord struct {
	Message   Message
	ExpiresAt time.Time
}
type fifoAttemptRecord struct {
	Messages  []Message
	ExpiresAt time.Time
}

type MemoryRepository struct {
	mu                sync.RWMutex
	queues            map[string]Queue
	messages          map[string][]Message
	messageIDs        map[string]struct{}
	receiptHandles    map[string]receiptBinding
	tags              map[string]map[string]string
	permissions       map[string]map[string]Permission
	origins           map[string]QueueRef
	moveTasks         map[string]MessageMoveTask
	messageSequences  map[string]uint64
	queueSequences    map[string]uint64
	fifoSequences     map[string]uint64
	fifoDedup         map[string]map[string]fifoDedupRecord
	fifoAttempts      map[string]map[string]fifoAttemptRecord
	deletedNames      map[string]struct{}
	namespaceRevision uint64
	redriveRevision   uint64
	queueSequence     uint64
	closed            bool
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		queues:           make(map[string]Queue),
		messages:         make(map[string][]Message),
		messageIDs:       make(map[string]struct{}),
		receiptHandles:   make(map[string]receiptBinding),
		tags:             make(map[string]map[string]string),
		permissions:      make(map[string]map[string]Permission),
		origins:          make(map[string]QueueRef),
		moveTasks:        make(map[string]MessageMoveTask),
		messageSequences: make(map[string]uint64),
		queueSequences:   make(map[string]uint64),
		fifoSequences:    make(map[string]uint64),
		fifoDedup:        make(map[string]map[string]fifoDedupRecord),
		fifoAttempts:     make(map[string]map[string]fifoAttemptRecord),
		deletedNames:     make(map[string]struct{}),
	}
}

func (r *MemoryRepository) Create(candidate Queue) (Queue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Queue{}, ErrRepositoryClosed
	}

	if existing, ok := r.queues[candidate.Name]; ok {
		if sameQueueCreationAttributes(existing, candidate) {
			return existing, nil
		}
		return Queue{}, ErrQueueAlreadyExists
	}
	if candidate.ID == "" {
		r.queueSequence++
		candidate.ID = fmt.Sprintf("q_%032x", r.queueSequence)
	}
	for _, existing := range r.queues {
		if existing.ID == candidate.ID {
			return Queue{}, ErrQueueIDUnavailable
		}
	}
	if r.queues == nil {
		r.queues = make(map[string]Queue)
	}
	_, wasDeleted := r.deletedNames[candidate.Name]
	candidate.LegacyURLAllowed = !wasDeleted
	r.queues[candidate.Name] = candidate
	r.tags[candidate.Name] = make(map[string]string)
	r.permissions[candidate.Name] = make(map[string]Permission)
	r.fifoDedup[candidate.Name] = make(map[string]fifoDedupRecord)
	r.fifoAttempts[candidate.Name] = make(map[string]fifoAttemptRecord)
	r.namespaceRevision++
	return candidate, nil
}

func sameQueueCreationAttributes(left, right Queue) bool {
	return left.Name == right.Name && left.VisibilityTimeout == right.VisibilityTimeout && left.DelaySeconds == right.DelaySeconds && left.MessageRetentionPeriod == right.MessageRetentionPeriod && left.FIFO == right.FIFO && left.ContentBasedDeduplication == right.ContentBasedDeduplication
}

func (r *MemoryRepository) Get(name string) (Queue, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return Queue{}, false, ErrRepositoryClosed
	}
	result, ok := r.queues[name]
	result.RedrivePolicy = cloneRedrivePolicy(result.RedrivePolicy)
	return result, ok, nil
}

func (r *MemoryRepository) GetByRef(ref QueueRef) (Queue, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return Queue{}, false, ErrRepositoryClosed
	}
	value, ok := r.queueByRef(ref)
	value.RedrivePolicy = cloneRedrivePolicy(value.RedrivePolicy)
	return value, ok, nil
}

func (r *MemoryRepository) queueByRef(ref QueueRef) (Queue, bool) {
	value, ok := r.queues[ref.Name]
	if !ok || ref.ID != "" && value.ID != ref.ID || ref.ID == "" && !value.LegacyURLAllowed {
		return Queue{}, false
	}
	return value, true
}

func (r *MemoryRepository) ListQueues(command ListQueuesCommand) (ListQueuesPage, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ListQueuesPage{}, ErrRepositoryClosed
	}
	if command.ExpectedRevision != nil && *command.ExpectedRevision != r.namespaceRevision {
		return ListQueuesPage{}, ErrInvalidPaginationToken
	}
	names := make([]string, 0, len(r.queues))
	for name := range r.queues {
		if len(name) >= len(command.Prefix) && name[:len(command.Prefix)] == command.Prefix && name > command.After {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	page := ListQueuesPage{Queues: []Queue{}, NamespaceRevision: r.namespaceRevision}
	limit := command.Limit
	if limit <= 0 || limit > len(names) {
		limit = len(names)
	}
	for _, name := range names[:limit] {
		page.Queues = append(page.Queues, r.queues[name])
	}
	page.HasMore = len(names) > limit
	return page, nil
}

func (r *MemoryRepository) DeleteQueue(ref QueueRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(ref); !ok {
		return ErrQueueDoesNotExist
	}
	for name, candidate := range r.queues {
		if name != ref.Name && candidate.RedrivePolicy != nil && candidate.RedrivePolicy.DeadLetterTarget == ref {
			return ErrQueueInUse
		}
	}
	for _, task := range r.moveTasks {
		if task.Status == MoveTaskRunning && (task.Source == ref || task.Destination != nil && *task.Destination == ref) {
			return ErrQueueInUse
		}
	}
	for _, message := range r.messages[ref.Name] {
		delete(r.origins, message.ID)
		delete(r.messageSequences, message.ID)
	}
	if r.queues[ref.Name].RedrivePolicy != nil {
		r.redriveRevision++
	}
	delete(r.queues, ref.Name)
	delete(r.messages, ref.Name)
	delete(r.tags, ref.Name)
	delete(r.permissions, ref.Name)
	delete(r.fifoDedup, ref.Name)
	delete(r.fifoAttempts, ref.Name)
	delete(r.fifoSequences, ref.Name)
	for handle, binding := range r.receiptHandles {
		if binding.QueueName == ref.Name {
			delete(r.receiptHandles, handle)
		}
	}
	r.deletedNames[ref.Name] = struct{}{}
	r.namespaceRevision++
	return nil
}

func (r *MemoryRepository) PurgeQueue(ref QueueRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(ref); !ok {
		return ErrQueueDoesNotExist
	}
	for _, message := range r.messages[ref.Name] {
		delete(r.origins, message.ID)
		delete(r.messageSequences, message.ID)
	}
	delete(r.messages, ref.Name)
	delete(r.fifoAttempts, ref.Name)
	r.fifoAttempts[ref.Name] = make(map[string]fifoAttemptRecord)
	for handle, binding := range r.receiptHandles {
		if binding.QueueName == ref.Name {
			delete(r.receiptHandles, handle)
		}
	}
	return nil
}

func (r *MemoryRepository) TagQueue(ref QueueRef, updates map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(ref); !ok {
		return ErrQueueDoesNotExist
	}
	current := r.tags[ref.Name]
	if len(current)+countNewTagKeys(current, updates) > 50 {
		return ErrOverLimit
	}
	for key, value := range updates {
		current[key] = value
	}
	return nil
}

func countNewTagKeys(current, updates map[string]string) int {
	count := 0
	for key := range updates {
		if _, ok := current[key]; !ok {
			count++
		}
	}
	return count
}

func (r *MemoryRepository) UntagQueue(ref QueueRef, keys []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(ref); !ok {
		return ErrQueueDoesNotExist
	}
	for _, key := range keys {
		delete(r.tags[ref.Name], key)
	}
	return nil
}

func (r *MemoryRepository) ListQueueTags(ref QueueRef) (map[string]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(ref); !ok {
		return nil, ErrQueueDoesNotExist
	}
	result := make(map[string]string, len(r.tags[ref.Name]))
	for key, value := range r.tags[ref.Name] {
		result[key] = value
	}
	return result, nil
}

func (r *MemoryRepository) AddPermission(ref QueueRef, permission Permission) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(ref); !ok {
		return ErrQueueDoesNotExist
	}
	current := r.permissions[ref.Name]
	if _, exists := current[permission.Label]; !exists && len(current) >= 20 {
		return ErrOverLimit
	}
	current[permission.Label] = clonePermission(permission)
	return nil
}

func (r *MemoryRepository) RemovePermission(ref QueueRef, label string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(ref); !ok {
		return ErrQueueDoesNotExist
	}
	delete(r.permissions[ref.Name], label)
	return nil
}

func (r *MemoryRepository) ListPermissions(ref QueueRef) ([]Permission, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(ref); !ok {
		return nil, ErrQueueDoesNotExist
	}
	labels := make([]string, 0, len(r.permissions[ref.Name]))
	for label := range r.permissions[ref.Name] {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	result := make([]Permission, 0, len(labels))
	for _, label := range labels {
		result = append(result, clonePermission(r.permissions[ref.Name][label]))
	}
	return result, nil
}

func clonePermission(value Permission) Permission {
	value.AWSAccountIDs = append([]string(nil), value.AWSAccountIDs...)
	value.Actions = append([]string(nil), value.Actions...)
	return value
}

func (r *MemoryRepository) SetAttributes(command QueueAttributesCommand) (Queue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Queue{}, ErrRepositoryClosed
	}
	value, ok := r.queueByRef(QueueRef{Name: command.QueueName, ID: command.QueueID})
	if !ok {
		return Queue{}, ErrQueueDoesNotExist
	}
	if command.VisibilityTimeout != nil {
		value.VisibilityTimeout = *command.VisibilityTimeout
	}
	if command.DelaySeconds != nil {
		value.DelaySeconds = *command.DelaySeconds
	}
	if command.MessageRetentionPeriod != nil {
		value.MessageRetentionPeriod = *command.MessageRetentionPeriod
	}
	if command.RedrivePolicySet {
		if command.RedrivePolicy != nil {
			if err := ValidateRedrivePolicy(command.RedrivePolicy); err != nil || command.RedrivePolicy.DeadLetterTarget == (QueueRef{Name: value.Name, ID: value.ID}) {
				return Queue{}, invalidRequest("RedrivePolicy target is invalid")
			}
			target, ok := r.queueByRef(command.RedrivePolicy.DeadLetterTarget)
			if !ok {
				return Queue{}, ErrQueueDoesNotExist
			}
			if target.FIFO != value.FIFO {
				return Queue{}, invalidRequest("RedrivePolicy source and target must have the same queue type")
			}
			seen := map[QueueRef]struct{}{{Name: value.Name, ID: value.ID}: {}}
			for cursor := command.RedrivePolicy.DeadLetterTarget; ; {
				if _, duplicate := seen[cursor]; duplicate {
					return Queue{}, invalidRequest("RedrivePolicy must not create a cycle")
				}
				seen[cursor] = struct{}{}
				next := r.queues[cursor.Name].RedrivePolicy
				if next == nil {
					break
				}
				cursor = next.DeadLetterTarget
			}
		}
		value.RedrivePolicy = cloneRedrivePolicy(command.RedrivePolicy)
		r.redriveRevision++
	}
	r.queues[command.QueueName] = value
	value.RedrivePolicy = cloneRedrivePolicy(value.RedrivePolicy)
	return value, nil
}

func (r *MemoryRepository) Enqueue(command EnqueueCommand) (Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Message{}, ErrRepositoryClosed
	}

	queueValue, ok := r.queueByRef(QueueRef{Name: command.QueueName, ID: command.QueueID})
	if !ok {
		return Message{}, ErrQueueDoesNotExist
	}
	message := cloneMessage(command.Message)
	if queueValue.FIFO {
		if message.MessageGroupID == "" || message.MessageDeduplicationID == "" {
			return Message{}, invalidRequest("FIFO message metadata is required")
		}
		for key, record := range r.fifoDedup[command.QueueName] {
			if !record.ExpiresAt.After(command.Now) {
				delete(r.fifoDedup[command.QueueName], key)
			}
		}
		if record, duplicate := r.fifoDedup[command.QueueName][message.MessageDeduplicationID]; duplicate {
			return cloneMessage(record.Message), nil
		}
		if _, exists := r.messageIDs[message.ID]; exists {
			return Message{}, ErrMessageIDExists
		}
		sequence, err := r.nextFIFOSequence(command.QueueName)
		if err != nil {
			return Message{}, err
		}
		message.SequenceNumber = sequence
	} else if message.MessageGroupID != "" || message.MessageDeduplicationID != "" || message.SequenceNumber != 0 {
		return Message{}, invalidRequest("FIFO message metadata is not valid for a Standard queue")
	} else if _, exists := r.messageIDs[message.ID]; exists {
		return Message{}, ErrMessageIDExists
	}
	if r.messages == nil {
		r.messages = make(map[string][]Message)
	}
	if r.messageIDs == nil {
		r.messageIDs = make(map[string]struct{})
	}
	delaySeconds := queueValue.DelaySeconds
	if command.DelaySeconds != nil {
		delaySeconds = *command.DelaySeconds
	}
	message.QueueName = command.QueueName
	message.SentAtMillis = command.Now.UnixMilli()
	message.AvailableAt = command.Now.Add(time.Duration(delaySeconds) * time.Second)
	message.ExpiresAt = command.Now.Add(time.Duration(queueValue.MessageRetentionPeriod) * time.Second)
	r.messages[command.QueueName] = append(r.messages[command.QueueName], message)
	r.queueSequences[command.QueueName]++
	r.messageSequences[message.ID] = r.queueSequences[command.QueueName]
	r.messageIDs[message.ID] = struct{}{}
	if queueValue.FIFO {
		r.fifoDedup[command.QueueName][message.MessageDeduplicationID] = fifoDedupRecord{Message: cloneMessage(message), ExpiresAt: command.Now.Add(5 * time.Minute)}
	}
	return cloneMessage(message), nil
}

func (r *MemoryRepository) Claim(input ClaimInput) (ClaimResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ClaimResult{}, ErrRepositoryClosed
	}

	queueValue, ok := r.queueByRef(QueueRef{Name: input.QueueName, ID: input.QueueID})
	if !ok {
		return ClaimResult{}, ErrQueueDoesNotExist
	}
	if !queueValue.FIFO && input.ReceiveRequestAttemptID != "" {
		return ClaimResult{}, invalidRequest("ReceiveRequestAttemptId is valid only for FIFO queues")
	}
	if queueValue.FIFO {
		for key, record := range r.fifoAttempts[input.QueueName] {
			if !record.ExpiresAt.After(input.Now) {
				delete(r.fifoAttempts[input.QueueName], key)
			}
		}
		if record, replay := r.fifoAttempts[input.QueueName][input.ReceiveRequestAttemptID]; replay && input.ReceiveRequestAttemptID != "" {
			return ClaimResult{Messages: cloneMessages(record.Messages), AttemptReplayed: true}, nil
		}
	}
	visibilityTimeout := time.Duration(queueValue.VisibilityTimeout) * time.Second
	if input.VisibilityTimeout != nil {
		visibilityTimeout = *input.VisibilityTimeout
	}
	if len(input.ReceiptHandles) < input.MaxNumberOfMessages {
		return ClaimResult{}, ErrReceiptUnavailable
	}
	seenHandles := make(map[string]struct{}, input.MaxNumberOfMessages)
	for _, handle := range input.ReceiptHandles[:input.MaxNumberOfMessages] {
		if handle == "" {
			return ClaimResult{}, ErrReceiptUnavailable
		}
		if _, duplicate := seenHandles[handle]; duplicate {
			return ClaimResult{}, ErrReceiptHandleExists
		}
		if _, exists := r.receiptHandles[handle]; exists {
			return ClaimResult{}, ErrReceiptHandleExists
		}
		seenHandles[handle] = struct{}{}
	}

	queueMessages := r.messages[input.QueueName]
	kept := make([]Message, 0, len(queueMessages))
	selected := make([]int, 0, input.MaxNumberOfMessages)
	movedTo := make([]QueueRef, 0)
	var nextTransition time.Time
	considerTransition := func(candidate time.Time) {
		if candidate.After(input.Now) && (nextTransition.IsZero() || candidate.Before(nextTransition)) {
			nextTransition = candidate
		}
	}
	seenGroups := make(map[string]struct{})
	for index := range queueMessages {
		message := queueMessages[index]
		if !message.ExpiresAt.After(input.Now) {
			delete(r.origins, message.ID)
			delete(r.messageSequences, message.ID)
			continue
		}
		if queueValue.FIFO {
			if _, blocked := seenGroups[message.MessageGroupID]; blocked {
				kept = append(kept, message)
				continue
			}
			seenGroups[message.MessageGroupID] = struct{}{}
		}
		if len(selected) == input.MaxNumberOfMessages {
			kept = append(kept, message)
			continue
		}
		if message.AvailableAt.After(input.Now) {
			considerTransition(message.AvailableAt)
			considerTransition(message.ExpiresAt)
			kept = append(kept, message)
			continue
		}
		if message.ReceiptHandle != "" && message.VisibilityDeadline.After(input.Now) {
			considerTransition(message.VisibilityDeadline)
			considerTransition(message.ExpiresAt)
			kept = append(kept, message)
			continue
		}
		if queueValue.RedrivePolicy != nil && message.ReceiveCount >= queueValue.RedrivePolicy.MaxReceiveCount {
			target := queueValue.RedrivePolicy.DeadLetterTarget
			targetQueue, ok := r.queueByRef(target)
			if !ok {
				return ClaimResult{}, ErrQueueDoesNotExist
			}
			if targetQueue.FIFO != queueValue.FIFO {
				return ClaimResult{}, fmt.Errorf("redrive queue type mismatch")
			}
			if queueValue.FIFO {
				sequence, err := r.nextFIFOSequence(targetQueue.Name)
				if err != nil {
					return ClaimResult{}, err
				}
				message.SequenceNumber = sequence
			}
			message.QueueName = target.Name
			message.ReceiptHandle = ""
			message.ReceiveCount = 0
			message.FirstReceivedAtMillis = 0
			message.VisibilityDeadline = time.Time{}
			message.AvailableAt = input.Now
			r.messages[target.Name] = append(r.messages[target.Name], message)
			r.queueSequences[target.Name]++
			r.messageSequences[message.ID] = r.queueSequences[target.Name]
			r.origins[message.ID] = QueueRef{Name: queueValue.Name, ID: queueValue.ID}
			movedTo = append(movedTo, target)
			continue
		}
		kept = append(kept, message)
		selected = append(selected, len(kept)-1)
	}
	r.messages[input.QueueName] = kept
	if len(selected) == 0 {
		result := ClaimResult{Messages: []Message{}, NextTransition: nextTransition, MovedTo: movedTo}
		if queueValue.FIFO && input.ReceiveRequestAttemptID != "" && input.StoreEmptyAttempt {
			r.fifoAttempts[input.QueueName][input.ReceiveRequestAttemptID] = fifoAttemptRecord{Messages: []Message{}, ExpiresAt: input.Now.Add(5 * time.Minute)}
		}
		return result, nil
	}
	if len(input.ReceiptHandles) < len(selected) {
		return ClaimResult{}, ErrReceiptUnavailable
	}
	if r.receiptHandles == nil {
		r.receiptHandles = make(map[string]receiptBinding)
	}

	seenCandidates := make(map[string]struct{}, len(selected))
	for index := range selected {
		handle := input.ReceiptHandles[index]
		if handle == "" {
			return ClaimResult{}, ErrReceiptUnavailable
		}
		if _, exists := seenCandidates[handle]; exists {
			return ClaimResult{}, ErrReceiptHandleExists
		}
		if _, exists := r.receiptHandles[handle]; exists {
			return ClaimResult{}, ErrReceiptHandleExists
		}
		seenCandidates[handle] = struct{}{}
	}

	claimed := make([]Message, 0, len(selected))
	for resultIndex, messageIndex := range selected {
		message := &kept[messageIndex]
		message.ReceiveCount++
		message.ReceiveGeneration++
		message.ReceiptHandle = input.ReceiptHandles[resultIndex]
		message.VisibilityDeadline = input.Now.Add(visibilityTimeout)
		if message.ReceiveCount == 1 {
			message.FirstReceivedAtMillis = input.Now.UnixMilli()
		}
		r.receiptHandles[message.ReceiptHandle] = receiptBinding{
			QueueName:  input.QueueName,
			QueueID:    queueValue.ID,
			MessageID:  message.ID,
			Generation: message.ReceiveGeneration,
		}
		claimed = append(claimed, cloneMessage(*message))
	}
	r.messages[input.QueueName] = kept
	if queueValue.FIFO && input.ReceiveRequestAttemptID != "" {
		r.fifoAttempts[input.QueueName][input.ReceiveRequestAttemptID] = fifoAttemptRecord{Messages: cloneMessages(claimed), ExpiresAt: input.Now.Add(5 * time.Minute)}
	}
	return ClaimResult{Messages: claimed, MovedTo: movedTo}, nil
}

func cloneMessages(values []Message) []Message {
	result := make([]Message, len(values))
	for index := range values {
		result[index] = cloneMessage(values[index])
	}
	return result
}

func (r *MemoryRepository) Delete(queueName, queueID, receiptHandle string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}

	queueValue, ok := r.queueByRef(QueueRef{Name: queueName, ID: queueID})
	if !ok {
		return ErrQueueDoesNotExist
	}
	binding, ok := r.receiptHandles[receiptHandle]
	if !ok || binding.QueueName != queueName || binding.QueueID != queueValue.ID {
		return ErrReceiptHandleIsInvalid
	}

	queueMessages := r.messages[queueName]
	for index := range queueMessages {
		message := &queueMessages[index]
		if message.ID != binding.MessageID {
			continue
		}
		if message.ReceiptHandle != receiptHandle || message.ReceiveGeneration != binding.Generation {
			return nil
		}
		messageID := message.ID
		copy(queueMessages[index:], queueMessages[index+1:])
		queueMessages[len(queueMessages)-1] = Message{}
		r.messages[queueName] = queueMessages[:len(queueMessages)-1]
		delete(r.origins, messageID)
		delete(r.messageSequences, messageID)
		return nil
	}

	return nil
}

func (r *MemoryRepository) ChangeVisibility(command ChangeVisibilityCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}

	queueValue, ok := r.queueByRef(QueueRef{Name: command.QueueName, ID: command.QueueID})
	if !ok {
		return ErrQueueDoesNotExist
	}
	binding, ok := r.receiptHandles[command.ReceiptHandle]
	if !ok || binding.QueueName != command.QueueName || binding.QueueID != queueValue.ID {
		return ErrReceiptHandleIsInvalid
	}
	if _, issued := r.messageIDs[binding.MessageID]; !issued {
		return ErrRepositoryCorrupt
	}

	queueMessages := r.messages[command.QueueName]
	for index := range queueMessages {
		message := &queueMessages[index]
		if message.ID != binding.MessageID {
			continue
		}
		if message.QueueName != command.QueueName {
			return ErrRepositoryCorrupt
		}
		if binding.Generation > message.ReceiveGeneration {
			return ErrRepositoryCorrupt
		}
		if !message.ExpiresAt.After(command.Now) {
			messageID := message.ID
			r.messages[command.QueueName] = removeMessageAt(queueMessages, index)
			delete(r.origins, messageID)
			delete(r.messageSequences, messageID)
			return nil
		}
		if binding.Generation < message.ReceiveGeneration {
			return nil
		}
		if message.ReceiptHandle != command.ReceiptHandle {
			return ErrRepositoryCorrupt
		}
		message.VisibilityDeadline = command.Now.Add(command.VisibilityTimeout)
		r.messages[command.QueueName] = queueMessages
		return nil
	}

	return nil
}

func (r *MemoryRepository) Expire(command ExpireCommand) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, ErrRepositoryClosed
	}
	expired := 0
	for queueName, messages := range r.messages {
		for _, message := range messages {
			if !message.ExpiresAt.After(command.Now) {
				delete(r.origins, message.ID)
				delete(r.messageSequences, message.ID)
			}
		}
		remaining := removeExpiredMessages(messages, command.Now)
		expired += len(messages) - len(remaining)
		r.messages[queueName] = remaining
	}
	return expired, nil
}

func removeExpiredMessages(messages []Message, now time.Time) []Message {
	write := 0
	for read := range messages {
		if !messages[read].ExpiresAt.After(now) {
			messages[read] = Message{}
			continue
		}
		if write != read {
			messages[write] = messages[read]
			messages[read] = Message{}
		}
		write++
	}
	return messages[:write]
}

func removeMessageAt(messages []Message, index int) []Message {
	copy(messages[index:], messages[index+1:])
	messages[len(messages)-1] = Message{}
	return messages[:len(messages)-1]
}

func (r *MemoryRepository) ListDeadLetterSources(command ListDeadLetterSourcesCommand) (ListDeadLetterSourcesPage, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ListDeadLetterSourcesPage{}, ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(command.Target); !ok {
		return ListDeadLetterSourcesPage{}, ErrQueueDoesNotExist
	}
	if command.ExpectedRevision != nil && *command.ExpectedRevision != r.redriveRevision {
		return ListDeadLetterSourcesPage{}, ErrInvalidPaginationToken
	}
	names := make([]string, 0)
	for name, candidate := range r.queues {
		if name > command.After && candidate.RedrivePolicy != nil && candidate.RedrivePolicy.DeadLetterTarget == command.Target {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	limit := command.Limit
	if limit < 1 || limit > len(names) {
		limit = len(names)
	}
	page := ListDeadLetterSourcesPage{Queues: make([]Queue, 0, limit), RedriveRevision: r.redriveRevision, HasMore: len(names) > limit}
	for _, name := range names[:limit] {
		value := r.queues[name]
		value.RedrivePolicy = cloneRedrivePolicy(value.RedrivePolicy)
		page.Queues = append(page.Queues, value)
	}
	return page, nil
}

func (r *MemoryRepository) StartMoveTask(command StartMoveTaskCommand) (MessageMoveTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return MessageMoveTask{}, ErrRepositoryClosed
	}
	if _, exists := r.moveTasks[command.Handle]; exists {
		return MessageMoveTask{}, ErrMessageIDExists
	}
	if _, ok := r.queueByRef(command.Source); !ok {
		return MessageMoveTask{}, ErrQueueDoesNotExist
	}
	isDLQ := false
	for _, candidate := range r.queues {
		if candidate.RedrivePolicy != nil && candidate.RedrivePolicy.DeadLetterTarget == command.Source {
			isDLQ = true
			break
		}
	}
	if !isDLQ {
		return MessageMoveTask{}, ErrQueueIsNotDeadLetter
	}
	if command.Destination != nil {
		if *command.Destination == command.Source {
			return MessageMoveTask{}, invalidRequest("move destination must differ from source")
		}
		destination, ok := r.queueByRef(*command.Destination)
		if !ok {
			return MessageMoveTask{}, ErrQueueDoesNotExist
		}
		source := r.queues[command.Source.Name]
		if destination.FIFO != source.FIFO {
			return MessageMoveTask{}, invalidRequest("move source and destination must have the same queue type")
		}
	}
	for _, task := range r.moveTasks {
		if task.Source == command.Source && task.Status == MoveTaskRunning {
			return MessageMoveTask{}, ErrMoveTaskAlreadyRunning
		}
	}
	boundary := r.queueSequences[command.Source.Name]
	var count uint64
	for _, message := range r.messages[command.Source.Name] {
		if r.messageSequences[message.ID] <= boundary {
			count++
		}
	}
	task := MessageMoveTask{Handle: command.Handle, Source: command.Source, Destination: cloneQueueRef(command.Destination), MaxNumberOfMessagesPerSecond: command.MaxMessagesPerSecond, StartedAtMillis: command.Now.UnixMilli(), Status: MoveTaskRunning, ApproximateNumberOfMessagesToMove: count, Boundary: boundary}
	if count == 0 {
		task.Status = MoveTaskCompleted
	}
	r.moveTasks[task.Handle] = task
	return cloneMoveTask(task), nil
}

func (r *MemoryRepository) ListMoveTasks(source QueueRef, limit int) ([]MessageMoveTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrRepositoryClosed
	}
	if _, ok := r.queueByRef(source); !ok {
		return nil, ErrQueueDoesNotExist
	}
	result := make([]MessageMoveTask, 0)
	for _, task := range r.moveTasks {
		if task.Source == source {
			result = append(result, cloneMoveTask(task))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].StartedAtMillis == result[j].StartedAtMillis {
			return result[i].Handle > result[j].Handle
		}
		return result[i].StartedAtMillis > result[j].StartedAtMillis
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (r *MemoryRepository) CancelMoveTask(handle string, now time.Time) (MessageMoveTask, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return MessageMoveTask{}, ErrRepositoryClosed
	}
	task, ok := r.moveTasks[handle]
	if !ok {
		return MessageMoveTask{}, ErrMoveTaskDoesNotExist
	}
	if task.Status != MoveTaskRunning {
		return MessageMoveTask{}, ErrMoveTaskNotRunning
	}
	task.Status = MoveTaskCancelled
	r.moveTasks[handle] = task
	return cloneMoveTask(task), nil
}

func (r *MemoryRepository) RunningMoveTasks() ([]MessageMoveTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrRepositoryClosed
	}
	result := []MessageMoveTask{}
	for _, task := range r.moveTasks {
		if task.Status == MoveTaskRunning {
			result = append(result, cloneMoveTask(task))
		}
	}
	return result, nil
}

func (r *MemoryRepository) MoveTaskStep(handle string, now time.Time) (MoveTaskStepResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return MoveTaskStepResult{}, ErrRepositoryClosed
	}
	task, ok := r.moveTasks[handle]
	if !ok {
		return MoveTaskStepResult{}, ErrMoveTaskDoesNotExist
	}
	if task.Status != MoveTaskRunning {
		return MoveTaskStepResult{Task: cloneMoveTask(task), Terminal: true}, nil
	}
	source, ok := r.queueByRef(task.Source)
	if !ok {
		task.Status, task.FailureReason = MoveTaskFailed, "source queue generation no longer exists"
		r.moveTasks[handle] = task
		return MoveTaskStepResult{Task: cloneMoveTask(task), Terminal: true}, nil
	}
	messages := r.messages[source.Name]
	index := -1
	for candidate := range messages {
		if r.messageSequences[messages[candidate].ID] <= task.Boundary {
			index = candidate
			break
		}
	}
	if index < 0 {
		task.Status = MoveTaskCompleted
		r.moveTasks[handle] = task
		return MoveTaskStepResult{Task: cloneMoveTask(task), Terminal: true}, nil
	}
	message := messages[index]
	var destination QueueRef
	if task.Destination != nil {
		destination = *task.Destination
	} else {
		destination = r.origins[message.ID]
	}
	destinationQueue, destinationOK := r.queueByRef(destination)
	if destination.ID == "" || !destinationOK {
		task.Status, task.FailureReason = MoveTaskFailed, "message original destination is unavailable"
		r.moveTasks[handle] = task
		return MoveTaskStepResult{Task: cloneMoveTask(task), Terminal: true}, nil
	}
	if destinationQueue.FIFO != source.FIFO {
		task.Status, task.FailureReason = MoveTaskFailed, "message destination queue type does not match source"
		r.moveTasks[handle] = task
		return MoveTaskStepResult{Task: cloneMoveTask(task), Terminal: true}, nil
	}
	r.messages[source.Name] = removeMessageAt(messages, index)
	if source.FIFO {
		sequence, err := r.nextFIFOSequence(destinationQueue.Name)
		if err != nil {
			return MoveTaskStepResult{}, err
		}
		message.SequenceNumber = sequence
	}
	message.QueueName = destination.Name
	message.SentAtMillis = now.UnixMilli()
	message.AvailableAt = now
	message.ExpiresAt = now.Add(time.Duration(destinationQueue.MessageRetentionPeriod) * time.Second)
	message.ReceiptHandle = ""
	message.ReceiveCount = 0
	message.FirstReceivedAtMillis = 0
	message.VisibilityDeadline = time.Time{}
	r.messages[destination.Name] = append(r.messages[destination.Name], message)
	r.queueSequences[destination.Name]++
	r.messageSequences[message.ID] = r.queueSequences[destination.Name]
	delete(r.origins, message.ID)
	task.ApproximateNumberOfMessagesMoved++
	r.moveTasks[handle] = task
	return MoveTaskStepResult{Task: cloneMoveTask(task), Destination: &destination}, nil
}

func (r *MemoryRepository) nextFIFOSequence(queueName string) (uint64, error) {
	if r.fifoSequences[queueName] == ^uint64(0) {
		return 0, fmt.Errorf("FIFO sequence exhausted")
	}
	r.fifoSequences[queueName]++
	return r.fifoSequences[queueName], nil
}

func cloneQueueRef(value *QueueRef) *QueueRef {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneMoveTask(value MessageMoveTask) MessageMoveTask {
	value.Destination = cloneQueueRef(value.Destination)
	return value
}

func (r *MemoryRepository) Health() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	return nil
}

func (r *MemoryRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *MemoryRepository) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.queues)
}

func (r *MemoryRepository) MessageCount(queueName string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.messages[queueName])
}

func (r *MemoryRepository) Messages(queueName string) []Message {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Message, len(r.messages[queueName]))
	for index := range r.messages[queueName] {
		result[index] = cloneMessage(r.messages[queueName][index])
	}
	return result
}
