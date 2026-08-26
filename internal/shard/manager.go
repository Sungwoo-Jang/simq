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

	"simq/internal/ownership"
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

func (m *Manager) hashedShardID(digest string) string {
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

func (m *Manager) routeForKey(key string) (shardID, digest string, epoch uint64, explicit bool, err error) {
	digest, ok := tenantDigest(key)
	if !ok {
		return m.defaultShard, "", 0, false, nil
	}
	control, ok := m.repositories[m.defaultShard].(ownership.ControlStore)
	if ok {
		record, found, readErr := control.LocalOwnership(digest)
		if readErr != nil {
			return "", digest, 0, false, readErr
		}
		if found {
			if _, configured := m.repositories[record.ShardID]; !configured {
				return "", digest, 0, true, fmt.Errorf("%w: ownership references an unknown shard", queue.ErrRepositoryCorrupt)
			}
			return record.ShardID, digest, record.Epoch, true, nil
		}
	}
	return m.hashedShardID(digest), digest, 1, false, nil
}

func (m *Manager) shardIDForKey(key string) string {
	shardID, _, _, _, err := m.routeForKey(key)
	if err != nil {
		return m.defaultShard
	}
	return shardID
}

func (m *Manager) repositoryForKey(key string) queue.Repository {
	shardID, _, _, _, err := m.routeForKey(key)
	if err != nil {
		return m.repositories[m.defaultShard]
	}
	return m.repositories[shardID]
}

func (m *Manager) activeRepositoryForKey(key string) (queue.Repository, error) {
	shardID, digest, epoch, explicit, err := m.routeForKey(key)
	if err != nil {
		return nil, err
	}
	repository := m.repositories[shardID]
	if digest == "" {
		return repository, nil
	}
	data, ok := repository.(ownership.DataStore)
	if !ok {
		return repository, nil
	}
	fence, found, err := data.LocalFence(digest)
	if err != nil {
		return nil, err
	}
	if !found {
		if !explicit || epoch == 1 && shardID == m.hashedShardID(digest) {
			return repository, nil
		}
		return nil, queue.ErrTenantMigrating
	}
	if fence.State != ownership.FenceActive || fence.Epoch != epoch {
		return nil, queue.ErrTenantMigrating
	}
	return repository, nil
}

func (m *Manager) redriveForKey(key string) (queue.RedriveRepository, error) {
	active, err := m.activeRepositoryForKey(key)
	if err != nil {
		return nil, err
	}
	repository, ok := active.(queue.RedriveRepository)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return repository, nil
}

func (m *Manager) Create(v queue.Queue) (queue.Queue, error) {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return queue.Queue{}, err
	}
	return repository.Create(v)
}
func (m *Manager) Get(name string) (queue.Queue, bool, error) {
	repository, err := m.activeRepositoryForKey(name)
	if err != nil {
		return queue.Queue{}, false, err
	}
	return repository.Get(name)
}
func (m *Manager) GetByRef(ref queue.QueueRef) (queue.Queue, bool, error) {
	repository, err := m.activeRepositoryForKey(ref.Name)
	if err != nil {
		return queue.Queue{}, false, err
	}
	return repository.GetByRef(ref)
}
func (m *Manager) ListQueues(v queue.ListQueuesCommand) (queue.ListQueuesPage, error) {
	repository, err := m.activeRepositoryForKey(v.Prefix)
	if err != nil {
		return queue.ListQueuesPage{}, err
	}
	return repository.ListQueues(v)
}
func (m *Manager) DeleteQueue(v queue.QueueRef) error {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return err
	}
	return repository.DeleteQueue(v)
}
func (m *Manager) PurgeQueue(v queue.QueueRef) error {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return err
	}
	return repository.PurgeQueue(v)
}
func (m *Manager) TagQueue(v queue.QueueRef, tags map[string]string) error {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return err
	}
	return repository.TagQueue(v, tags)
}
func (m *Manager) UntagQueue(v queue.QueueRef, keys []string) error {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return err
	}
	return repository.UntagQueue(v, keys)
}
func (m *Manager) ListQueueTags(v queue.QueueRef) (map[string]string, error) {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return nil, err
	}
	return repository.ListQueueTags(v)
}
func (m *Manager) AddPermission(v queue.QueueRef, p queue.Permission) error {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return err
	}
	return repository.AddPermission(v, p)
}
func (m *Manager) RemovePermission(v queue.QueueRef, label string) error {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return err
	}
	return repository.RemovePermission(v, label)
}
func (m *Manager) ListPermissions(v queue.QueueRef) ([]queue.Permission, error) {
	repository, err := m.activeRepositoryForKey(v.Name)
	if err != nil {
		return nil, err
	}
	return repository.ListPermissions(v)
}
func (m *Manager) SetAttributes(v queue.QueueAttributesCommand) (queue.Queue, error) {
	repository, err := m.activeRepositoryForKey(v.QueueName)
	if err != nil {
		return queue.Queue{}, err
	}
	return repository.SetAttributes(v)
}
func (m *Manager) Enqueue(v queue.EnqueueCommand) (queue.Message, error) {
	repository, err := m.activeRepositoryForKey(v.QueueName)
	if err != nil {
		return queue.Message{}, err
	}
	return repository.Enqueue(v)
}
func (m *Manager) Claim(v queue.ClaimInput) (queue.ClaimResult, error) {
	repository, err := m.activeRepositoryForKey(v.QueueName)
	if err != nil {
		return queue.ClaimResult{}, err
	}
	return repository.Claim(v)
}
func (m *Manager) Delete(name, id, receipt string) error {
	repository, err := m.activeRepositoryForKey(name)
	if err != nil {
		return err
	}
	return repository.Delete(name, id, receipt)
}
func (m *Manager) ChangeVisibility(v queue.ChangeVisibilityCommand) error {
	repository, err := m.activeRepositoryForKey(v.QueueName)
	if err != nil {
		return err
	}
	return repository.ChangeVisibility(v)
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
	shardID, _, _, _, err := m.routeForKey(key)
	if err != nil {
		return true, false, ""
	}
	repository := m.repositories[shardID]
	routing, ok := repository.(queue.ClusterRoutingRepository)
	if !ok {
		return false, true, ""
	}
	return true, routing.IsLeader(), routing.LeaderAPIURL()
}

func (m *Manager) controlStore() (ownership.ControlStore, error) {
	value, ok := m.repositories[m.defaultShard].(ownership.ControlStore)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return value, nil
}

func (m *Manager) dataStore(shardID string) (ownership.DataStore, error) {
	repository, ok := m.repositories[shardID]
	if !ok {
		return nil, fmt.Errorf("unknown shard %q", shardID)
	}
	value, ok := repository.(ownership.DataStore)
	if !ok {
		return nil, queue.ErrRepositoryUnavailable
	}
	return value, nil
}

func (m *Manager) BeginTenantMigration(id, digest, destination string) (ownership.Migration, error) {
	if !ownership.ValidMigrationID(id) || !ownership.ValidDigest(digest) {
		return ownership.Migration{}, &queue.InvalidRequestError{Message: "migration ID or tenant digest is invalid"}
	}
	if _, ok := m.repositories[destination]; !ok {
		return ownership.Migration{}, &queue.InvalidRequestError{Message: "destination shard is not configured"}
	}
	control, err := m.controlStore()
	if err != nil {
		return ownership.Migration{}, err
	}
	current, found, err := control.Ownership(digest)
	if err != nil {
		return ownership.Migration{}, err
	}
	if !found {
		current = ownership.Record{Version: ownership.RecordVersion, TenantDigest: digest, ShardID: m.hashedShardID(digest), Epoch: 1}
	}
	if current.ShardID == destination {
		return ownership.Migration{}, &queue.InvalidRequestError{Message: "destination shard already owns the tenant"}
	}
	return control.BeginMigration(ownership.BeginCommand{MigrationID: id, TenantDigest: digest, SourceShard: current.ShardID, DestinationShard: destination, SourceEpoch: current.Epoch})
}

func (m *Manager) ListTenantMigrations(limit int) ([]ownership.Migration, error) {
	control, err := m.controlStore()
	if err != nil {
		return nil, err
	}
	return control.ListMigrations(limit)
}

func (m *Manager) TenantMigrationStatus(id string) (ownership.Status, error) {
	if !ownership.ValidMigrationID(id) {
		return ownership.Status{}, &queue.InvalidRequestError{Message: "migration ID is invalid"}
	}
	control, err := m.controlStore()
	if err != nil {
		return ownership.Status{}, err
	}
	migration, found, err := control.LocalMigration(id)
	if err != nil {
		return ownership.Status{}, err
	}
	if !found {
		return ownership.Status{}, queue.ErrMigrationDoesNotExist
	}
	nextShard, nextAction := "", ""
	switch migration.Phase {
	case ownership.PhaseFreezing:
		fence, exists, readErr := m.localFence(migration.SourceShard, migration.TenantDigest)
		if readErr != nil {
			return ownership.Status{}, readErr
		}
		if exists && fence.MigrationID == migration.ID && fence.State == ownership.FenceFrozen {
			nextShard = m.defaultShard
			nextAction = "record-bundle"
		} else {
			nextShard = migration.SourceShard
			nextAction = "freeze"
		}
	case ownership.PhasePreparing:
		fence, exists, readErr := m.localFence(migration.DestinationShard, migration.TenantDigest)
		if readErr != nil {
			return ownership.Status{}, readErr
		}
		if exists && fence.MigrationID == migration.ID && fence.State == ownership.FencePrepared {
			nextShard = m.defaultShard
			nextAction = "record-prepared"
		} else {
			nextShard = migration.DestinationShard
			nextAction = "prepare"
		}
	case ownership.PhaseCuttingOver:
		nextShard = m.defaultShard
		nextAction = "cutover"
	case ownership.PhaseActivating:
		fence, exists, readErr := m.localFence(migration.DestinationShard, migration.TenantDigest)
		if readErr != nil {
			return ownership.Status{}, readErr
		}
		if exists && fence.MigrationID == migration.ID && fence.State == ownership.FenceActive {
			nextShard = m.defaultShard
			nextAction = "record-active"
		} else {
			nextShard = migration.DestinationShard
			nextAction = "activate"
		}
	case ownership.PhaseCleaning:
		fence, exists, readErr := m.localFence(migration.SourceShard, migration.TenantDigest)
		if readErr != nil {
			return ownership.Status{}, readErr
		}
		if exists && fence.MigrationID == migration.ID && fence.State == ownership.FenceMoved {
			nextShard = m.defaultShard
			nextAction = "complete"
		} else {
			nextShard = migration.SourceShard
			nextAction = "cleanup"
		}
	case ownership.PhaseAborting, ownership.PhaseAborted:
		destinationFence, destinationExists, readErr := m.localFence(migration.DestinationShard, migration.TenantDigest)
		if readErr != nil {
			return ownership.Status{}, readErr
		}
		if destinationExists && destinationFence.MigrationID == migration.ID {
			nextShard = migration.DestinationShard
			nextAction = "abort-destination"
		} else if sourceFence, sourceExists, sourceErr := m.localFence(migration.SourceShard, migration.TenantDigest); sourceErr != nil {
			return ownership.Status{}, sourceErr
		} else if sourceExists && sourceFence.MigrationID == migration.ID {
			nextShard = migration.SourceShard
			nextAction = "abort-source"
		} else if migration.Phase == ownership.PhaseAborting {
			nextShard = m.defaultShard
			nextAction = "finish-abort"
		}
	}
	status := ownership.Status{Migration: migration, NextAction: nextAction, NextShard: nextShard}
	if nextShard != "" {
		if routing, ok := m.repositories[nextShard].(queue.ClusterRoutingRepository); ok {
			status.LocalLeader, status.NextLeaderURL = routing.IsLeader(), routing.LeaderAPIURL()
		} else {
			status.LocalLeader = true
		}
	}
	return status, nil
}

func (m *Manager) localFence(shardID, digest string) (ownership.Fence, bool, error) {
	data, err := m.dataStore(shardID)
	if err != nil {
		return ownership.Fence{}, false, err
	}
	return data.LocalFence(digest)
}

type MigrationLeaderError struct{ LeaderURL string }

func (e *MigrationLeaderError) Error() string {
	return "tenant migration step must use the required shard leader"
}
func (e *MigrationLeaderError) Unwrap() error { return queue.ErrRepositoryUnavailable }

func (m *Manager) AdvanceTenantMigration(id string) (ownership.Status, error) {
	status, err := m.TenantMigrationStatus(id)
	if err != nil {
		return ownership.Status{}, err
	}
	if status.NextShard == "" {
		return status, nil
	}
	if !status.LocalLeader {
		return status, &MigrationLeaderError{LeaderURL: status.NextLeaderURL}
	}
	migration := status.Migration
	control, err := m.controlStore()
	if err != nil {
		return status, err
	}
	switch migration.Phase {
	case ownership.PhaseFreezing:
		if status.NextAction == "freeze" {
			data, dataErr := m.dataStore(migration.SourceShard)
			if dataErr != nil {
				return status, dataErr
			}
			if _, dataErr = data.FreezeAndCapture(migration); dataErr != nil {
				return status, dataErr
			}
		} else {
			data, dataErr := m.dataStore(migration.SourceShard)
			if dataErr != nil {
				return status, dataErr
			}
			bundle, found, readErr := data.LocalBundle(migration.ID)
			if readErr != nil || !found {
				return status, firstMigrationError(readErr)
			}
			if _, err = control.TransitionMigration(ownership.TransitionCommand{MigrationID: migration.ID, ExpectedPhase: migration.Phase, NextPhase: ownership.PhasePreparing, BundleHash: bundle.Hash}); err != nil {
				return status, err
			}
		}
	case ownership.PhasePreparing:
		if status.NextAction == "prepare" {
			source, dataErr := m.dataStore(migration.SourceShard)
			if dataErr != nil {
				return status, dataErr
			}
			bundle, found, readErr := source.LocalBundle(migration.ID)
			if readErr != nil || !found {
				return status, firstMigrationError(readErr)
			}
			data, dataErr := m.dataStore(migration.DestinationShard)
			if dataErr != nil {
				return status, dataErr
			}
			if _, dataErr = data.Prepare(migration, bundle); dataErr != nil {
				return status, dataErr
			}
		} else if _, err = control.TransitionMigration(ownership.TransitionCommand{MigrationID: migration.ID, ExpectedPhase: migration.Phase, NextPhase: ownership.PhaseCuttingOver, BundleHash: migration.BundleHash}); err != nil {
			return status, err
		}
	case ownership.PhaseCuttingOver:
		if _, err = control.TransitionMigration(ownership.TransitionCommand{MigrationID: migration.ID, ExpectedPhase: migration.Phase, NextPhase: ownership.PhaseActivating, BundleHash: migration.BundleHash}); err != nil {
			return status, err
		}
	case ownership.PhaseActivating:
		if status.NextAction == "activate" {
			data, dataErr := m.dataStore(migration.DestinationShard)
			if dataErr != nil {
				return status, dataErr
			}
			if _, dataErr = data.Activate(migration); dataErr != nil {
				return status, dataErr
			}
		} else if _, err = control.TransitionMigration(ownership.TransitionCommand{MigrationID: migration.ID, ExpectedPhase: migration.Phase, NextPhase: ownership.PhaseCleaning, BundleHash: migration.BundleHash}); err != nil {
			return status, err
		}
	case ownership.PhaseCleaning:
		if status.NextAction == "cleanup" {
			data, dataErr := m.dataStore(migration.SourceShard)
			if dataErr != nil {
				return status, dataErr
			}
			if _, dataErr = data.Cleanup(migration); dataErr != nil {
				return status, dataErr
			}
		} else if _, err = control.TransitionMigration(ownership.TransitionCommand{MigrationID: migration.ID, ExpectedPhase: migration.Phase, NextPhase: ownership.PhaseCompleted, BundleHash: migration.BundleHash}); err != nil {
			return status, err
		}
	case ownership.PhaseAborting:
		if status.NextAction == "abort-destination" {
			data, dataErr := m.dataStore(migration.DestinationShard)
			if dataErr != nil {
				return status, dataErr
			}
			if dataErr = data.AbortDestination(migration); dataErr != nil {
				return status, dataErr
			}
		} else if status.NextAction == "abort-source" {
			data, dataErr := m.dataStore(migration.SourceShard)
			if dataErr != nil {
				return status, dataErr
			}
			if dataErr = data.AbortSource(migration); dataErr != nil {
				return status, dataErr
			}
		} else if _, err = control.TransitionMigration(ownership.TransitionCommand{MigrationID: migration.ID, ExpectedPhase: migration.Phase, NextPhase: ownership.PhaseAborted, BundleHash: migration.BundleHash}); err != nil {
			return status, err
		}
	case ownership.PhaseAborted:
		if status.NextAction == "abort-destination" {
			data, dataErr := m.dataStore(migration.DestinationShard)
			if dataErr != nil {
				return status, dataErr
			}
			if dataErr = data.AbortDestination(migration); dataErr != nil {
				return status, dataErr
			}
		} else if status.NextAction == "abort-source" {
			data, dataErr := m.dataStore(migration.SourceShard)
			if dataErr != nil {
				return status, dataErr
			}
			if dataErr = data.AbortSource(migration); dataErr != nil {
				return status, dataErr
			}
		}
	}
	return m.TenantMigrationStatus(id)
}

func firstMigrationError(err error) error {
	if err != nil {
		return err
	}
	return queue.ErrTenantMigrating
}

func (m *Manager) AbortTenantMigration(id string) (ownership.Status, error) {
	status, err := m.TenantMigrationStatus(id)
	if err != nil {
		return ownership.Status{}, err
	}
	if status.Migration.Phase == ownership.PhaseAborted {
		return status, nil
	}
	if status.Migration.Phase == ownership.PhaseActivating || status.Migration.Phase == ownership.PhaseCleaning || status.Migration.Phase == ownership.PhaseCompleted {
		return status, queue.ErrTenantOwnershipConflict
	}
	routing, ok := m.repositories[m.defaultShard].(queue.ClusterRoutingRepository)
	if ok && !routing.IsLeader() {
		return status, &MigrationLeaderError{LeaderURL: routing.LeaderAPIURL()}
	}
	control, err := m.controlStore()
	if err != nil {
		return status, err
	}
	if status.Migration.Phase != ownership.PhaseAborting {
		if _, err := control.TransitionMigration(ownership.TransitionCommand{MigrationID: id, ExpectedPhase: status.Migration.Phase, NextPhase: ownership.PhaseAborting, BundleHash: status.Migration.BundleHash}); err != nil {
			return status, err
		}
	}
	return m.TenantMigrationStatus(id)
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
var _ queue.TenantMigrationAdminRepository = (*Manager)(nil)
var _ queue.StorageQuotaRepository = (*Manager)(nil)
