package tenant

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"simq/internal/queue"
)

const namespacePrefixLength = 68 // "tn_" + 64 hex characters + "_"

type PayloadCipher struct {
	active string
	keys   map[string]cipher.AEAD
}

func NewPayloadCipher(active string, keys map[string][]byte) (*PayloadCipher, error) {
	if active == "" {
		return nil, fmt.Errorf("active encryption key ID is required")
	}
	result := &PayloadCipher{active: active, keys: make(map[string]cipher.AEAD, len(keys))}
	for id, key := range keys {
		if id == "" || strings.ContainsAny(id, ":\r\n") || len(key) != 32 {
			return nil, fmt.Errorf("encryption key %q must be 32 bytes and have a safe ID", id)
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		result.keys[id] = aead
	}
	if result.keys[active] == nil {
		return nil, fmt.Errorf("active encryption key is absent from key ring")
	}
	return result, nil
}

type Repository struct {
	underlying   queue.Repository
	namespace    string
	tenantDigest string
	legacyTenant string
	bound        bool
	cipher       *PayloadCipher
}

func New(underlying queue.Repository, payloadCipher *PayloadCipher, legacyTenant ...string) *Repository {
	legacy := ""
	if len(legacyTenant) > 0 {
		legacy = legacyTenant[0]
	}
	return &Repository{underlying: underlying, cipher: payloadCipher, legacyTenant: legacy}
}
func (r *Repository) ForTenant(id string) queue.Repository {
	digest := sha256.Sum256([]byte(id))
	digestText := hex.EncodeToString(digest[:])
	namespace := "tn_" + digestText + "_"
	if id == r.legacyTenant {
		namespace = ""
	}
	return &Repository{underlying: r.underlying, namespace: namespace, tenantDigest: digestText, legacyTenant: r.legacyTenant, bound: true, cipher: r.cipher}
}
func (r *Repository) require() error {
	if !r.bound {
		return queue.ErrTenantRequired
	}
	return nil
}
func hasNamespacePrefix(value string) bool {
	if len(value) < namespacePrefixLength || !strings.HasPrefix(value, "tn_") || value[namespacePrefixLength-1] != '_' {
		return false
	}
	_, err := hex.DecodeString(value[3 : namespacePrefixLength-1])
	return err == nil
}
func (r *Repository) requireName(value string) error {
	if err := r.require(); err != nil {
		return err
	}
	if r.namespace == "" && hasNamespacePrefix(value) {
		return &queue.InvalidRequestError{Message: "queue name uses a reserved internal namespace"}
	}
	return nil
}
func (r *Repository) name(value string) string { return r.namespace + value }
func stripName(value string) string {
	if hasNamespacePrefix(value) {
		return value[namespacePrefixLength:]
	}
	return value
}
func (r *Repository) ref(value queue.QueueRef) queue.QueueRef {
	value.Name = r.name(value.Name)
	return value
}
func stripRef(value queue.QueueRef) queue.QueueRef { value.Name = stripName(value.Name); return value }
func (r *Repository) queueOut(value queue.Queue) queue.Queue {
	value.Name = stripName(value.Name)
	if value.RedrivePolicy != nil {
		copy := *value.RedrivePolicy
		copy.DeadLetterTarget = stripRef(copy.DeadLetterTarget)
		value.RedrivePolicy = &copy
	}
	return value
}
func stripTask(value queue.MessageMoveTask) queue.MessageMoveTask {
	value.Handle = stripName(value.Handle)
	value.Source = stripRef(value.Source)
	if value.Destination != nil {
		copy := stripRef(*value.Destination)
		value.Destination = &copy
	}
	return value
}

type encryptedPayload struct {
	Body                   string                            `json:"body"`
	Attributes             map[string]queue.MessageAttribute `json:"attributes"`
	MD5OfBody              string                            `json:"md5_of_body"`
	MD5OfMessageAttributes string                            `json:"md5_of_message_attributes"`
}

func (r *Repository) encrypt(message queue.Message) (queue.Message, error) {
	if r.cipher == nil {
		return message, nil
	}
	var payload bytes.Buffer
	encoder := json.NewEncoder(&payload)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(encryptedPayload{Body: message.Body, Attributes: message.MessageAttributes, MD5OfBody: message.MD5OfBody, MD5OfMessageAttributes: message.MD5OfMessageAttributes}); err != nil {
		return queue.Message{}, err
	}
	aead := r.cipher.keys[r.cipher.active]
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return queue.Message{}, err
	}
	aad := []byte(r.tenantDigest + "\x00" + message.ID)
	sealed := aead.Seal(nonce, nonce, payload.Bytes(), aad)
	message.PlaintextMD5OfBody = message.MD5OfBody
	message.PlaintextMD5OfMessageAttributes = message.MD5OfMessageAttributes
	message.Body = "simqenc1:" + r.cipher.active + ":" + base64.RawURLEncoding.EncodeToString(sealed)
	digest := md5.Sum([]byte(message.Body))
	message.MD5OfBody = hex.EncodeToString(digest[:])
	message.MessageAttributes = nil
	message.MD5OfMessageAttributes = ""
	return message, nil
}
func (r *Repository) decryptAcknowledgement(message queue.Message) (queue.Message, error) {
	if message.Body != "" {
		return r.decrypt(message)
	}
	message.QueueName = stripName(message.QueueName)
	message.ID = stripName(message.ID)
	message.ReceiptHandle = stripName(message.ReceiptHandle)
	return message, nil
}
func (r *Repository) decrypt(message queue.Message) (queue.Message, error) {
	if r.cipher == nil {
		message.QueueName = stripName(message.QueueName)
		message.ID = stripName(message.ID)
		message.ReceiptHandle = stripName(message.ReceiptHandle)
		return message, nil
	}
	parts := strings.Split(message.Body, ":")
	if len(parts) != 3 || parts[0] != "simqenc1" {
		return queue.Message{}, queue.ErrRepositoryCorrupt
	}
	aead := r.cipher.keys[parts[1]]
	if aead == nil {
		return queue.Message{}, queue.ErrRepositoryCorrupt
	}
	sealed, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sealed) < aead.NonceSize() {
		return queue.Message{}, queue.ErrRepositoryCorrupt
	}
	aad := []byte(r.tenantDigest + "\x00" + message.ID)
	plain, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], aad)
	if err != nil {
		return queue.Message{}, queue.ErrRepositoryCorrupt
	}
	var payload encryptedPayload
	if err := json.Unmarshal(plain, &payload); err != nil {
		return queue.Message{}, queue.ErrRepositoryCorrupt
	}
	message.Body = payload.Body
	message.MessageAttributes = payload.Attributes
	message.MD5OfBody = payload.MD5OfBody
	message.MD5OfMessageAttributes = payload.MD5OfMessageAttributes
	message.ID = stripName(message.ID)
	message.ReceiptHandle = stripName(message.ReceiptHandle)
	message.QueueName = stripName(message.QueueName)
	return message, nil
}

func (r *Repository) Create(value queue.Queue) (queue.Queue, error) {
	if err := r.requireName(value.Name); err != nil {
		return queue.Queue{}, err
	}
	value.Name = r.name(value.Name)
	if value.RedrivePolicy != nil {
		if err := r.requireName(value.RedrivePolicy.DeadLetterTarget.Name); err != nil {
			return queue.Queue{}, err
		}
		copy := *value.RedrivePolicy
		copy.DeadLetterTarget = r.ref(copy.DeadLetterTarget)
		value.RedrivePolicy = &copy
	}
	result, err := r.underlying.Create(value)
	return r.queueOut(result), err
}
func (r *Repository) Get(name string) (queue.Queue, bool, error) {
	if err := r.requireName(name); err != nil {
		return queue.Queue{}, false, err
	}
	result, found, err := r.underlying.Get(r.name(name))
	return r.queueOut(result), found, err
}
func (r *Repository) GetByRef(ref queue.QueueRef) (queue.Queue, bool, error) {
	if err := r.requireName(ref.Name); err != nil {
		return queue.Queue{}, false, err
	}
	result, found, err := r.underlying.GetByRef(r.ref(ref))
	return r.queueOut(result), found, err
}
func (r *Repository) ListQueues(command queue.ListQueuesCommand) (queue.ListQueuesPage, error) {
	if err := r.requireName(command.Prefix); err != nil {
		return queue.ListQueuesPage{}, err
	}
	if command.After != "" {
		if err := r.requireName(command.After); err != nil {
			return queue.ListQueuesPage{}, err
		}
	}
	if r.namespace == "" {
		return r.listLegacyQueues(command)
	}
	command.Prefix = r.name(command.Prefix)
	if command.After != "" {
		command.After = r.name(command.After)
	}
	page, err := r.underlying.ListQueues(command)
	if err != nil {
		return queue.ListQueuesPage{}, err
	}
	for index := range page.Queues {
		page.Queues[index] = r.queueOut(page.Queues[index])
	}
	return page, nil
}
func (r *Repository) listLegacyQueues(command queue.ListQueuesCommand) (queue.ListQueuesPage, error) {
	wanted := command.Limit
	if wanted < 1 {
		wanted = 1000
	}
	result := queue.ListQueuesPage{Queues: []queue.Queue{}}
	after := command.After
	for {
		page, err := r.underlying.ListQueues(queue.ListQueuesCommand{Prefix: command.Prefix, After: after, Limit: 1000, ExpectedRevision: command.ExpectedRevision})
		if err != nil {
			return queue.ListQueuesPage{}, err
		}
		result.NamespaceRevision = page.NamespaceRevision
		for _, candidate := range page.Queues {
			after = candidate.Name
			if hasNamespacePrefix(candidate.Name) {
				continue
			}
			if len(result.Queues) == wanted {
				result.HasMore = true
				return result, nil
			}
			result.Queues = append(result.Queues, candidate)
		}
		if !page.HasMore {
			return result, nil
		}
		if len(page.Queues) == 0 {
			return queue.ListQueuesPage{}, queue.ErrRepositoryCorrupt
		}
	}
}
func (r *Repository) DeleteQueue(ref queue.QueueRef) error {
	if err := r.requireName(ref.Name); err != nil {
		return err
	}
	return r.underlying.DeleteQueue(r.ref(ref))
}
func (r *Repository) PurgeQueue(ref queue.QueueRef) error {
	if err := r.requireName(ref.Name); err != nil {
		return err
	}
	return r.underlying.PurgeQueue(r.ref(ref))
}
func (r *Repository) TagQueue(ref queue.QueueRef, tags map[string]string) error {
	if err := r.requireName(ref.Name); err != nil {
		return err
	}
	return r.underlying.TagQueue(r.ref(ref), tags)
}
func (r *Repository) UntagQueue(ref queue.QueueRef, keys []string) error {
	if err := r.requireName(ref.Name); err != nil {
		return err
	}
	return r.underlying.UntagQueue(r.ref(ref), keys)
}
func (r *Repository) ListQueueTags(ref queue.QueueRef) (map[string]string, error) {
	if err := r.requireName(ref.Name); err != nil {
		return nil, err
	}
	return r.underlying.ListQueueTags(r.ref(ref))
}
func (r *Repository) AddPermission(ref queue.QueueRef, p queue.Permission) error {
	if err := r.requireName(ref.Name); err != nil {
		return err
	}
	return r.underlying.AddPermission(r.ref(ref), p)
}
func (r *Repository) RemovePermission(ref queue.QueueRef, label string) error {
	if err := r.requireName(ref.Name); err != nil {
		return err
	}
	return r.underlying.RemovePermission(r.ref(ref), label)
}
func (r *Repository) ListPermissions(ref queue.QueueRef) ([]queue.Permission, error) {
	if err := r.requireName(ref.Name); err != nil {
		return nil, err
	}
	return r.underlying.ListPermissions(r.ref(ref))
}
func (r *Repository) SetAttributes(command queue.QueueAttributesCommand) (queue.Queue, error) {
	if err := r.requireName(command.QueueName); err != nil {
		return queue.Queue{}, err
	}
	command.QueueName = r.name(command.QueueName)
	if command.RedrivePolicy != nil {
		if err := r.requireName(command.RedrivePolicy.DeadLetterTarget.Name); err != nil {
			return queue.Queue{}, err
		}
		copy := *command.RedrivePolicy
		copy.DeadLetterTarget = r.ref(copy.DeadLetterTarget)
		command.RedrivePolicy = &copy
	}
	result, err := r.underlying.SetAttributes(command)
	return r.queueOut(result), err
}
func (r *Repository) Enqueue(command queue.EnqueueCommand) (queue.Message, error) {
	if err := r.requireName(command.QueueName); err != nil {
		return queue.Message{}, err
	}
	command.QueueName = r.name(command.QueueName)
	command.Message.QueueName = command.QueueName
	command.Message.ID = r.name(command.Message.ID)
	encrypted, err := r.encrypt(command.Message)
	if err != nil {
		return queue.Message{}, err
	}
	command.Message = encrypted
	result, err := r.underlying.Enqueue(command)
	if err != nil {
		return queue.Message{}, err
	}
	return r.decryptAcknowledgement(result)
}
func (r *Repository) Claim(input queue.ClaimInput) (queue.ClaimResult, error) {
	if err := r.requireName(input.QueueName); err != nil {
		return queue.ClaimResult{}, err
	}
	input.QueueName = r.name(input.QueueName)
	for index := range input.ReceiptHandles {
		input.ReceiptHandles[index] = r.name(input.ReceiptHandles[index])
	}
	result, err := r.underlying.Claim(input)
	if err != nil {
		return queue.ClaimResult{}, err
	}
	for index := range result.Messages {
		result.Messages[index], err = r.decrypt(result.Messages[index])
		if err != nil {
			return queue.ClaimResult{}, err
		}
	}
	for index := range result.MovedTo {
		result.MovedTo[index] = stripRef(result.MovedTo[index])
	}
	return result, nil
}
func (r *Repository) Delete(name, id, receipt string) error {
	if err := r.requireName(name); err != nil {
		return err
	}
	return r.underlying.Delete(r.name(name), id, r.name(receipt))
}
func (r *Repository) ChangeVisibility(command queue.ChangeVisibilityCommand) error {
	if err := r.requireName(command.QueueName); err != nil {
		return err
	}
	command.QueueName = r.name(command.QueueName)
	command.ReceiptHandle = r.name(command.ReceiptHandle)
	return r.underlying.ChangeVisibility(command)
}
func (r *Repository) Expire(command queue.ExpireCommand) (int, error) {
	return r.underlying.Expire(command)
}
func (r *Repository) Health() error { return r.underlying.Health() }
func (r *Repository) Close() error  { return r.underlying.Close() }

func (r *Repository) redrive() (queue.RedriveRepository, error) {
	value, ok := r.underlying.(queue.RedriveRepository)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return value, nil
}
func (r *Repository) ListDeadLetterSources(command queue.ListDeadLetterSourcesCommand) (queue.ListDeadLetterSourcesPage, error) {
	if err := r.requireName(command.Target.Name); err != nil {
		return queue.ListDeadLetterSourcesPage{}, err
	}
	repository, err := r.redrive()
	if err != nil {
		return queue.ListDeadLetterSourcesPage{}, err
	}
	command.Target = r.ref(command.Target)
	if command.After != "" {
		command.After = r.name(command.After)
	}
	page, err := repository.ListDeadLetterSources(command)
	if err == nil {
		for index := range page.Queues {
			page.Queues[index] = r.queueOut(page.Queues[index])
		}
	}
	return page, err
}
func (r *Repository) StartMoveTask(command queue.StartMoveTaskCommand) (queue.MessageMoveTask, error) {
	if err := r.requireName(command.Source.Name); err != nil {
		return queue.MessageMoveTask{}, err
	}
	if command.Destination != nil {
		if err := r.requireName(command.Destination.Name); err != nil {
			return queue.MessageMoveTask{}, err
		}
	}
	repository, err := r.redrive()
	if err != nil {
		return queue.MessageMoveTask{}, err
	}
	command.Handle = r.name(command.Handle)
	command.Source = r.ref(command.Source)
	if command.Destination != nil {
		copy := r.ref(*command.Destination)
		command.Destination = &copy
	}
	task, err := repository.StartMoveTask(command)
	return stripTask(task), err
}
func (r *Repository) ListMoveTasks(ref queue.QueueRef, limit int) ([]queue.MessageMoveTask, error) {
	if err := r.requireName(ref.Name); err != nil {
		return nil, err
	}
	repository, err := r.redrive()
	if err != nil {
		return nil, err
	}
	tasks, err := repository.ListMoveTasks(r.ref(ref), limit)
	for index := range tasks {
		tasks[index] = stripTask(tasks[index])
	}
	return tasks, err
}
func (r *Repository) CancelMoveTask(handle string, now time.Time) (queue.MessageMoveTask, error) {
	if err := r.require(); err != nil {
		return queue.MessageMoveTask{}, err
	}
	repository, err := r.redrive()
	if err != nil {
		return queue.MessageMoveTask{}, err
	}
	task, err := repository.CancelMoveTask(r.name(handle), now)
	return stripTask(task), err
}
func (r *Repository) MoveTaskStep(handle string, now time.Time) (queue.MoveTaskStepResult, error) {
	repository, err := r.redrive()
	if err != nil {
		return queue.MoveTaskStepResult{}, err
	}
	if r.bound {
		handle = r.name(handle)
	}
	result, err := repository.MoveTaskStep(handle, now)
	if r.bound {
		result.Task = stripTask(result.Task)
		if result.Destination != nil {
			copy := stripRef(*result.Destination)
			result.Destination = &copy
		}
	}
	return result, err
}
func (r *Repository) RunningMoveTasks() ([]queue.MessageMoveTask, error) {
	repository, err := r.redrive()
	if err != nil {
		return nil, err
	}
	tasks, err := repository.RunningMoveTasks()
	if !r.bound {
		return tasks, err
	}
	filtered := make([]queue.MessageMoveTask, 0, len(tasks))
	for _, task := range tasks {
		if r.namespace == "" && !hasNamespacePrefix(task.Source.Name) || r.namespace != "" && strings.HasPrefix(task.Source.Name, r.namespace) {
			filtered = append(filtered, stripTask(task))
		}
	}
	return filtered, err
}

func (r *Repository) ForOperation(id string) queue.Repository {
	if scoped, ok := r.underlying.(queue.OperationScopedRepository); ok {
		return &Repository{underlying: scoped.ForOperation(id), namespace: r.namespace, tenantDigest: r.tenantDigest, legacyTenant: r.legacyTenant, bound: r.bound, cipher: r.cipher}
	}
	return r
}
func (r *Repository) Clustered() bool {
	if routed, ok := r.underlying.(queue.KeyRoutingRepository); ok {
		clustered, _, _ := routed.RouteForKey(r.namespace)
		return clustered
	}
	value, ok := r.underlying.(queue.OperationScopedRepository)
	return ok && value.Clustered()
}
func (r *Repository) IsLeader() bool {
	if routed, ok := r.underlying.(queue.KeyRoutingRepository); ok {
		_, leader, _ := routed.RouteForKey(r.namespace)
		return leader
	}
	value, ok := r.underlying.(queue.ClusterRoutingRepository)
	return ok && value.IsLeader()
}
func (r *Repository) LeaderAPIURL() string {
	if routed, ok := r.underlying.(queue.KeyRoutingRepository); ok {
		_, _, leaderURL := routed.RouteForKey(r.namespace)
		return leaderURL
	}
	value, ok := r.underlying.(queue.ClusterRoutingRepository)
	if !ok {
		return ""
	}
	return value.LeaderAPIURL()
}
func (r *Repository) admin() (queue.ClusterAdminRepository, error) {
	value, ok := r.underlying.(queue.ClusterAdminRepository)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return value, nil
}
func (r *Repository) ClusterMembers() ([]queue.ClusterMember, error) {
	value, err := r.admin()
	if err != nil {
		return nil, err
	}
	return value.ClusterMembers()
}
func (r *Repository) AddClusterVoter(id, address string) error {
	value, err := r.admin()
	if err != nil {
		return err
	}
	return value.AddClusterVoter(id, address)
}
func (r *Repository) AddClusterNonvoter(id, address string) error {
	value, err := r.admin()
	if err != nil {
		return err
	}
	return value.AddClusterNonvoter(id, address)
}
func (r *Repository) DemoteClusterVoter(id string) error {
	value, err := r.admin()
	if err != nil {
		return err
	}
	return value.DemoteClusterVoter(id)
}
func (r *Repository) RemoveClusterServer(id string) error {
	value, err := r.admin()
	if err != nil {
		return err
	}
	return value.RemoveClusterServer(id)
}
func (r *Repository) TriggerClusterSnapshot() error {
	value, err := r.admin()
	if err != nil {
		return err
	}
	return value.TriggerClusterSnapshot()
}

func (r *Repository) shardAdmin() (queue.ShardAdminRepository, error) {
	value, ok := r.underlying.(queue.ShardAdminRepository)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return value, nil
}
func (r *Repository) ShardStatuses() []queue.ShardStatus {
	value, err := r.shardAdmin()
	if err != nil {
		return nil
	}
	return value.ShardStatuses()
}
func (r *Repository) ShardMembers(shard string) ([]queue.ClusterMember, error) {
	value, err := r.shardAdmin()
	if err != nil {
		return nil, err
	}
	return value.ShardMembers(shard)
}
func (r *Repository) AddShardVoter(shard, id, address string) error {
	value, err := r.shardAdmin()
	if err != nil {
		return err
	}
	return value.AddShardVoter(shard, id, address)
}
func (r *Repository) AddShardNonvoter(shard, id, address string) error {
	value, err := r.shardAdmin()
	if err != nil {
		return err
	}
	return value.AddShardNonvoter(shard, id, address)
}
func (r *Repository) DemoteShardVoter(shard, id string) error {
	value, err := r.shardAdmin()
	if err != nil {
		return err
	}
	return value.DemoteShardVoter(shard, id)
}
func (r *Repository) RemoveShardServer(shard, id string) error {
	value, err := r.shardAdmin()
	if err != nil {
		return err
	}
	return value.RemoveShardServer(shard, id)
}
func (r *Repository) TriggerShardSnapshot(shard string) error {
	value, err := r.shardAdmin()
	if err != nil {
		return err
	}
	return value.TriggerShardSnapshot(shard)
}

var _ queue.Repository = (*Repository)(nil)
var _ queue.RedriveRepository = (*Repository)(nil)
var _ queue.TenantScopedRepository = (*Repository)(nil)
var _ queue.ShardAdminRepository = (*Repository)(nil)

func IsCiphertext(value string) bool { return strings.HasPrefix(value, "simqenc1:") }
