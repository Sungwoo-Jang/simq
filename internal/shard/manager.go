package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"simq/internal/queue"
)

const tenantNamespaceLength = 68

type Config struct {
	DefaultShard    string
	CatalogRevision string
	Repositories    map[string]queue.Repository
	Placements      map[string]map[string]Placement
}

type Placement struct{ Address, FailureDomain string }

type Manager struct {
	defaultShard    string
	catalogRevision string
	repositories    map[string]queue.Repository
	shardIDs        []string
	closeOnce       *sync.Once
	closeErr        *error
	placements      map[string]map[string]Placement
}

func New(config Config) (*Manager, error) {
	if len(config.Repositories) == 0 {
		return nil, fmt.Errorf("at least one shard repository is required")
	}
	if config.DefaultShard == "" {
		return nil, fmt.Errorf("default shard is required")
	}
	if _, ok := config.Repositories[config.DefaultShard]; !ok {
		return nil, fmt.Errorf("default shard %q is not configured", config.DefaultShard)
	}
	ids := make([]string, 0, len(config.Repositories))
	copyRepositories := make(map[string]queue.Repository, len(config.Repositories))
	for id, repository := range config.Repositories {
		if !validShardID(id) || repository == nil {
			return nil, fmt.Errorf("invalid shard %q", id)
		}
		ids = append(ids, id)
		copyRepositories[id] = repository
	}
	sort.Strings(ids)
	revision := config.CatalogRevision
	if revision == "" {
		hash := sha256.Sum256([]byte(strings.Join(ids, "\x00") + "\x00" + config.DefaultShard))
		revision = hex.EncodeToString(hash[:16])
	}
	closeOnce := &sync.Once{}
	var closeErr error
	return &Manager{defaultShard: config.DefaultShard, catalogRevision: revision, repositories: copyRepositories, shardIDs: ids, closeOnce: closeOnce, closeErr: &closeErr, placements: clonePlacements(config.Placements)}, nil
}

func clonePlacements(source map[string]map[string]Placement) map[string]map[string]Placement {
	result := make(map[string]map[string]Placement, len(source))
	for shard, values := range source {
		copyValues := make(map[string]Placement, len(values))
		for id, value := range values {
			copyValues[id] = value
		}
		result[shard] = copyValues
	}
	return result
}

func validShardID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func tenantDigest(key string) (string, bool) {
	if len(key) < tenantNamespaceLength || !strings.HasPrefix(key, "tn_") || key[67] != '_' {
		return "", false
	}
	digest := key[3:67]
	decoded, err := hex.DecodeString(digest)
	return digest, err == nil && len(decoded) == 32
}

func (m *Manager) shardIDForKey(key string) string {
	digest, ok := tenantDigest(key)
	if !ok {
		return m.defaultShard
	}
	selected := m.shardIDs[0]
	var best uint64
	for index, id := range m.shardIDs {
		score := sha256.Sum256([]byte(digest + "\x00" + id))
		value := binary.BigEndian.Uint64(score[:8])
		if index == 0 || value > best {
			selected, best = id, value
		}
	}
	return selected
}

func (m *Manager) repositoryForKey(key string) queue.Repository {
	return m.repositories[m.shardIDForKey(key)]
}

func (m *Manager) redriveForKey(key string) (queue.RedriveRepository, error) {
	repository, ok := m.repositoryForKey(key).(queue.RedriveRepository)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return repository, nil
}

func (m *Manager) Create(v queue.Queue) (queue.Queue, error) {
	return m.repositoryForKey(v.Name).Create(v)
}
func (m *Manager) Get(name string) (queue.Queue, bool, error) {
	return m.repositoryForKey(name).Get(name)
}
func (m *Manager) GetByRef(ref queue.QueueRef) (queue.Queue, bool, error) {
	return m.repositoryForKey(ref.Name).GetByRef(ref)
}
func (m *Manager) ListQueues(v queue.ListQueuesCommand) (queue.ListQueuesPage, error) {
	return m.repositoryForKey(v.Prefix).ListQueues(v)
}
func (m *Manager) DeleteQueue(v queue.QueueRef) error {
	return m.repositoryForKey(v.Name).DeleteQueue(v)
}
func (m *Manager) PurgeQueue(v queue.QueueRef) error { return m.repositoryForKey(v.Name).PurgeQueue(v) }
func (m *Manager) TagQueue(v queue.QueueRef, tags map[string]string) error {
	return m.repositoryForKey(v.Name).TagQueue(v, tags)
}
func (m *Manager) UntagQueue(v queue.QueueRef, keys []string) error {
	return m.repositoryForKey(v.Name).UntagQueue(v, keys)
}
func (m *Manager) ListQueueTags(v queue.QueueRef) (map[string]string, error) {
	return m.repositoryForKey(v.Name).ListQueueTags(v)
}
func (m *Manager) AddPermission(v queue.QueueRef, p queue.Permission) error {
	return m.repositoryForKey(v.Name).AddPermission(v, p)
}
func (m *Manager) RemovePermission(v queue.QueueRef, label string) error {
	return m.repositoryForKey(v.Name).RemovePermission(v, label)
}
func (m *Manager) ListPermissions(v queue.QueueRef) ([]queue.Permission, error) {
	return m.repositoryForKey(v.Name).ListPermissions(v)
}
func (m *Manager) SetAttributes(v queue.QueueAttributesCommand) (queue.Queue, error) {
	return m.repositoryForKey(v.QueueName).SetAttributes(v)
}
func (m *Manager) Enqueue(v queue.EnqueueCommand) (queue.Message, error) {
	return m.repositoryForKey(v.QueueName).Enqueue(v)
}
func (m *Manager) Claim(v queue.ClaimInput) (queue.ClaimResult, error) {
	return m.repositoryForKey(v.QueueName).Claim(v)
}
func (m *Manager) Delete(name, id, receipt string) error {
	return m.repositoryForKey(name).Delete(name, id, receipt)
}
func (m *Manager) ChangeVisibility(v queue.ChangeVisibilityCommand) error {
	return m.repositoryForKey(v.QueueName).ChangeVisibility(v)
}
func (m *Manager) Expire(v queue.ExpireCommand) (int, error) {
	total := 0
	for _, id := range m.shardIDs {
		repository := m.repositories[id]
		if routing, ok := repository.(queue.ClusterRoutingRepository); ok && !routing.IsLeader() {
			continue
		}
		count, err := repository.Expire(v)
		if err != nil {
			return total, err
		}
		total += count
	}
	return total, nil
}
func (m *Manager) Health() error {
	for _, id := range m.shardIDs {
		if err := m.repositories[id].Health(); err != nil {
			return fmt.Errorf("shard %s: %w", id, err)
		}
	}
	return nil
}
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		values := make([]error, 0, len(m.shardIDs))
		for _, id := range m.shardIDs {
			values = append(values, m.repositories[id].Close())
		}
		*m.closeErr = errors.Join(values...)
	})
	return *m.closeErr
}

func (m *Manager) ListDeadLetterSources(v queue.ListDeadLetterSourcesCommand) (queue.ListDeadLetterSourcesPage, error) {
	repository, err := m.redriveForKey(v.Target.Name)
	if err != nil {
		return queue.ListDeadLetterSourcesPage{}, err
	}
	return repository.ListDeadLetterSources(v)
}
func (m *Manager) StartMoveTask(v queue.StartMoveTaskCommand) (queue.MessageMoveTask, error) {
	repository, err := m.redriveForKey(v.Source.Name)
	if err != nil {
		return queue.MessageMoveTask{}, err
	}
	return repository.StartMoveTask(v)
}
func (m *Manager) ListMoveTasks(v queue.QueueRef, limit int) ([]queue.MessageMoveTask, error) {
	repository, err := m.redriveForKey(v.Name)
	if err != nil {
		return nil, err
	}
	return repository.ListMoveTasks(v, limit)
}
func (m *Manager) CancelMoveTask(handle string, now time.Time) (queue.MessageMoveTask, error) {
	repository, err := m.redriveForKey(handle)
	if err != nil {
		return queue.MessageMoveTask{}, err
	}
	return repository.CancelMoveTask(handle, now)
}
func (m *Manager) MoveTaskStep(handle string, now time.Time) (queue.MoveTaskStepResult, error) {
	repository, err := m.redriveForKey(handle)
	if err != nil {
		return queue.MoveTaskStepResult{}, err
	}
	return repository.MoveTaskStep(handle, now)
}
func (m *Manager) RunningMoveTasks() ([]queue.MessageMoveTask, error) {
	result := []queue.MessageMoveTask{}
	for _, id := range m.shardIDs {
		repository := m.repositories[id]
		if routing, ok := repository.(queue.ClusterRoutingRepository); ok && !routing.IsLeader() {
			continue
		}
		redrive, ok := repository.(queue.RedriveRepository)
		if !ok {
			return nil, queue.ErrRepositoryUnavailable
		}
		tasks, err := redrive.RunningMoveTasks()
		if err != nil {
			return nil, err
		}
		result = append(result, tasks...)
	}
	return result, nil
}

func (m *Manager) ForOperation(id string) queue.Repository {
	repositories := make(map[string]queue.Repository, len(m.repositories))
	for shardID, repository := range m.repositories {
		if scoped, ok := repository.(queue.OperationScopedRepository); ok {
			repositories[shardID] = scoped.ForOperation(id)
		} else {
			repositories[shardID] = repository
		}
	}
	return &Manager{defaultShard: m.defaultShard, catalogRevision: m.catalogRevision, repositories: repositories, shardIDs: append([]string(nil), m.shardIDs...), closeOnce: m.closeOnce, closeErr: m.closeErr, placements: m.placements}
}
func (m *Manager) Clustered() bool { return true }
func (m *Manager) RouteForKey(key string) (bool, bool, string) {
	repository := m.repositoryForKey(key)
	routing, ok := repository.(queue.ClusterRoutingRepository)
	if !ok {
		return false, true, ""
	}
	return true, routing.IsLeader(), routing.LeaderAPIURL()
}
func (m *Manager) IsLeader() bool       { _, leader, _ := m.RouteForKey(""); return leader }
func (m *Manager) LeaderAPIURL() string { _, _, value := m.RouteForKey(""); return value }

func (m *Manager) defaultAdmin() (queue.ClusterAdminRepository, error) {
	value, ok := m.repositories[m.defaultShard].(queue.ClusterAdminRepository)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return value, nil
}
func (m *Manager) ClusterMembers() ([]queue.ClusterMember, error) {
	v, e := m.defaultAdmin()
	if e != nil {
		return nil, e
	}
	return v.ClusterMembers()
}
func (m *Manager) AddClusterVoter(id, address string) error {
	return m.AddShardVoter(m.defaultShard, id, address)
}
func (m *Manager) AddClusterNonvoter(id, address string) error {
	return m.AddShardNonvoter(m.defaultShard, id, address)
}
func (m *Manager) DemoteClusterVoter(id string) error { return m.DemoteShardVoter(m.defaultShard, id) }
func (m *Manager) RemoveClusterServer(id string) error {
	return m.RemoveShardServer(m.defaultShard, id)
}
func (m *Manager) TriggerClusterSnapshot() error { return m.TriggerShardSnapshot(m.defaultShard) }

func (m *Manager) shardAdmin(id string) (queue.ClusterAdminRepository, error) {
	repository, ok := m.repositories[id]
	if !ok {
		return nil, fmt.Errorf("unknown shard %q", id)
	}
	admin, ok := repository.(queue.ClusterAdminRepository)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return admin, nil
}
func (m *Manager) ShardStatuses() []queue.ShardStatus {
	result := make([]queue.ShardStatus, 0, len(m.shardIDs))
	for _, id := range m.shardIDs {
		routing, _ := m.repositories[id].(queue.ClusterRoutingRepository)
		status := queue.ShardStatus{ID: id, CatalogRevision: m.catalogRevision}
		if routing != nil {
			status.Leader, status.LeaderURL = routing.IsLeader(), routing.LeaderAPIURL()
		}
		result = append(result, status)
	}
	return result
}
func (m *Manager) ShardMembers(id string) ([]queue.ClusterMember, error) {
	v, e := m.shardAdmin(id)
	if e != nil {
		return nil, e
	}
	return v.ClusterMembers()
}
func (m *Manager) AddShardVoter(shard, id, address string) error {
	v, e := m.shardAdmin(shard)
	if e != nil {
		return e
	}
	if err := m.validatePlacementChange(shard, id, address, "add-voter"); err != nil {
		return err
	}
	return v.AddClusterVoter(id, address)
}
func (m *Manager) AddShardNonvoter(shard, id, address string) error {
	v, e := m.shardAdmin(shard)
	if e != nil {
		return e
	}
	if err := m.validatePlacementChange(shard, id, address, "add-nonvoter"); err != nil {
		return err
	}
	return v.AddClusterNonvoter(id, address)
}
func (m *Manager) DemoteShardVoter(shard, id string) error {
	v, e := m.shardAdmin(shard)
	if e != nil {
		return e
	}
	if err := m.validatePlacementChange(shard, id, "", "demote"); err != nil {
		return err
	}
	return v.DemoteClusterVoter(id)
}
func (m *Manager) RemoveShardServer(shard, id string) error {
	v, e := m.shardAdmin(shard)
	if e != nil {
		return e
	}
	if err := m.validatePlacementChange(shard, id, "", "remove"); err != nil {
		return err
	}
	return v.RemoveClusterServer(id)
}

func (m *Manager) validatePlacementChange(shard, id, address, action string) error {
	planned, ok := m.placements[shard][id]
	if !ok {
		return fmt.Errorf("node %q is absent from shard %q placement manifest", id, shard)
	}
	if (action == "add-voter" || action == "add-nonvoter") && planned.Address != address {
		return fmt.Errorf("Raft address does not match shard placement manifest")
	}
	if action == "add-nonvoter" {
		return nil
	}
	members, err := m.ShardMembers(shard)
	if err != nil {
		return err
	}
	voters := make(map[string]struct{})
	for _, member := range members {
		if strings.EqualFold(member.Suffrage, "voter") {
			voters[member.ID] = struct{}{}
		}
	}
	switch action {
	case "add-voter":
		voters[id] = struct{}{}
	case "demote", "remove":
		delete(voters, id)
	}
	if len(voters) < 3 {
		return fmt.Errorf("placement change would leave fewer than three voters")
	}
	quorum := len(voters)/2 + 1
	domains := make(map[string]int)
	for voter := range voters {
		placement, exists := m.placements[shard][voter]
		if !exists || placement.FailureDomain == "" {
			return fmt.Errorf("voter %q lacks a failure-domain placement", voter)
		}
		domains[placement.FailureDomain]++
	}
	for domain, count := range domains {
		if count >= quorum {
			return fmt.Errorf("placement change would place quorum in failure domain %q", domain)
		}
	}
	return nil
}
func (m *Manager) TriggerShardSnapshot(id string) error {
	v, e := m.shardAdmin(id)
	if e != nil {
		return e
	}
	return v.TriggerClusterSnapshot()
}
func (m *Manager) SetStorageQuota(quota queue.StorageQuota) {
	for _, id := range m.shardIDs {
		if value, ok := m.repositories[id].(queue.StorageQuotaRepository); ok {
			value.SetStorageQuota(quota)
		}
	}
}

var _ queue.Repository = (*Manager)(nil)
var _ queue.RedriveRepository = (*Manager)(nil)
var _ queue.OperationScopedRepository = (*Manager)(nil)
var _ queue.KeyRoutingRepository = (*Manager)(nil)
var _ queue.ClusterAdminRepository = (*Manager)(nil)
var _ queue.ShardAdminRepository = (*Manager)(nil)
var _ queue.StorageQuotaRepository = (*Manager)(nil)
