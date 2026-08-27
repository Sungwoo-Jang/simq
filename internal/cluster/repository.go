package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"

	"simq/internal/ownership"
	"simq/internal/queue"
	"simq/internal/storage/boltrepo"
	"simq/internal/topology"
)

const commandVersion uint32 = 3

type Config struct {
	NodeID, BindAddress, AdvertiseAddress, DataDir string
	Bootstrap                                      bool
	ApplyTimeout, ReadTimeout, OpenTimeout         time.Duration
	SnapshotRetain                                 int
	APIURLs                                        map[string]string
	InitialVoters                                  map[string]string
	TLSCertificateFile, TLSPrivateKeyFile          string
	TLSCAFile, TLSServerName                       string
	CatalogRevision                                string
}

type core struct {
	raft                      *raft.Raft
	local                     *boltrepo.Repository
	store                     *durableStore
	transport                 *raft.NetworkTransport
	applyTimeout, readTimeout time.Duration
	closeOnce                 sync.Once
	closeErr                  error
	apiURLs                   map[raft.ServerID]string
	stateMu                   sync.RWMutex
	quotaMu                   sync.RWMutex
	storageQuota              queue.StorageQuota
}

type Repository struct {
	core     *core
	prefix   string
	sequence *atomic.Uint64
}

type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "repository is unavailable: no elected cluster leader"
	}
	return "repository is unavailable: request must use leader " + e.Leader
}
func (e *NotLeaderError) Unwrap() error { return queue.ErrRepositoryUnavailable }

func Open(config Config) (*Repository, error) {
	if config.NodeID == "" || config.BindAddress == "" || config.DataDir == "" {
		return nil, fmt.Errorf("cluster node ID, bind address, and data directory are required")
	}
	if config.AdvertiseAddress == "" {
		config.AdvertiseAddress = config.BindAddress
	}
	if config.ApplyTimeout <= 0 {
		config.ApplyTimeout = 5 * time.Second
	}
	if config.ReadTimeout <= 0 {
		config.ReadTimeout = 3 * time.Second
	}
	if config.OpenTimeout <= 0 {
		config.OpenTimeout = time.Second
	}
	if config.SnapshotRetain <= 0 {
		config.SnapshotRetain = 2
	}
	if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
		return nil, err
	}
	local, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(config.DataDir, "fsm.db"), OpenTimeout: config.OpenTimeout})
	if err != nil {
		return nil, err
	}
	if config.CatalogRevision != "" {
		if err := local.BindShardCatalogRevision(config.CatalogRevision); err != nil {
			_ = local.Close()
			return nil, err
		}
	}
	store, err := openDurableStore(filepath.Join(config.DataDir, "raft.db"), config.OpenTimeout)
	if err != nil {
		_ = local.Close()
		return nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(config.DataDir, "snapshots"), config.SnapshotRetain, io.Discard)
	if err != nil {
		_ = store.Close()
		_ = local.Close()
		return nil, err
	}
	transport, err := newTransport(config)
	if err != nil {
		_ = store.Close()
		_ = local.Close()
		return nil, err
	}
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(config.NodeID)
	rc.ShutdownOnRemove = true
	apiURLs := make(map[raft.ServerID]string, len(config.APIURLs))
	for id, url := range config.APIURLs {
		apiURLs[raft.ServerID(id)] = url
	}
	c := &core{local: local, store: store, transport: transport, applyTimeout: config.ApplyTimeout, readTimeout: config.ReadTimeout, apiURLs: apiURLs}
	instance, err := raft.NewRaft(rc, &fsm{local: local, stateMu: &c.stateMu, catalogRevision: config.CatalogRevision}, store, store, snapshots, transport)
	if err != nil {
		_ = transport.Close()
		_ = store.Close()
		_ = local.Close()
		return nil, err
	}
	c.raft = instance
	existing, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		_ = c.close()
		return nil, err
	}
	if config.Bootstrap && !existing {
		servers := []raft.Server{{ID: raft.ServerID(config.NodeID), Address: transport.LocalAddr(), Suffrage: raft.Voter}}
		if len(config.InitialVoters) > 0 {
			servers = servers[:0]
			ids := make([]string, 0, len(config.InitialVoters))
			for id := range config.InitialVoters {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(config.InitialVoters[id]), Suffrage: raft.Voter})
			}
		}
		selfFound := false
		for _, server := range servers {
			if server.ID == raft.ServerID(config.NodeID) && server.Address == transport.LocalAddr() {
				selfFound = true
			}
		}
		if !selfFound {
			_ = c.close()
			return nil, fmt.Errorf("bootstrap initial voters must contain the local node and advertised address")
		}
		if err := instance.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			_ = c.close()
			return nil, err
		}
	}
	return &Repository{core: c, prefix: config.NodeID + "/internal", sequence: &atomic.Uint64{}}, nil
}

func (r *Repository) ForOperation(id string) queue.Repository {
	return &Repository{core: r.core, prefix: id, sequence: &atomic.Uint64{}}
}
func (r *Repository) Clustered() bool       { return true }
func (r *Repository) LeaderAddress() string { return string(r.core.raft.Leader()) }
func (r *Repository) IsLeader() bool        { return r.core.raft.State() == raft.Leader }
func (r *Repository) LeaderAPIURL() string {
	_, id := r.core.raft.LeaderWithID()
	return r.core.apiURLs[id]
}
func (r *Repository) SetStorageQuota(quota queue.StorageQuota) {
	r.core.quotaMu.Lock()
	r.core.storageQuota = quota
	r.core.quotaMu.Unlock()
	r.core.local.SetStorageQuota(quota)
}
func (r *Repository) State() raft.RaftState { return r.core.raft.State() }
func (r *Repository) AddVoter(id, address string) error {
	if err := r.linearizableRead(); err != nil {
		return err
	}
	return r.core.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, r.core.applyTimeout).Error()
}
func (r *Repository) AddNonvoter(id, address string) error {
	if err := r.linearizableRead(); err != nil {
		return err
	}
	return r.core.raft.AddNonvoter(raft.ServerID(id), raft.ServerAddress(address), 0, r.core.applyTimeout).Error()
}
func (r *Repository) DemoteVoter(id string) error {
	if err := r.linearizableRead(); err != nil {
		return err
	}
	return r.core.raft.DemoteVoter(raft.ServerID(id), 0, r.core.applyTimeout).Error()
}
func (r *Repository) RemoveServer(id string) error {
	if err := r.linearizableRead(); err != nil {
		return err
	}
	return r.core.raft.RemoveServer(raft.ServerID(id), 0, r.core.applyTimeout).Error()
}
func (r *Repository) Configuration() (raft.Configuration, error) {
	if err := r.linearizableRead(); err != nil {
		return raft.Configuration{}, err
	}
	future := r.core.raft.GetConfiguration()
	return future.Configuration(), future.Error()
}
func (r *Repository) Snapshot() error { return r.core.raft.Snapshot().Error() }
func (r *Repository) TriggerClusterSnapshot() error {
	if err := r.linearizableRead(); err != nil {
		return err
	}
	return r.Snapshot()
}
func (r *Repository) ClusterMembers() ([]queue.ClusterMember, error) {
	configuration, err := r.Configuration()
	if err != nil {
		return nil, err
	}
	members := make([]queue.ClusterMember, 0, len(configuration.Servers))
	for _, server := range configuration.Servers {
		members = append(members, queue.ClusterMember{ID: string(server.ID), Address: string(server.Address), Suffrage: server.Suffrage.String()})
	}
	return members, nil
}
func (r *Repository) AddClusterVoter(id, address string) error    { return r.AddVoter(id, address) }
func (r *Repository) AddClusterNonvoter(id, address string) error { return r.AddNonvoter(id, address) }
func (r *Repository) DemoteClusterVoter(id string) error          { return r.DemoteVoter(id) }
func (r *Repository) RemoveClusterServer(id string) error         { return r.RemoveServer(id) }

func (c *core) close() error {
	c.closeOnce.Do(func() {
		c.closeErr = errors.Join(c.raft.Shutdown().Error(), c.transport.Close(), c.store.Close(), c.local.Close())
	})
	return c.closeErr
}
func (r *Repository) Close() error { return r.core.close() }
func (r *Repository) nextID() string {
	return r.prefix + "/" + strconv.FormatUint(r.sequence.Add(1), 10)
}
func (r *Repository) linearizableRead() error {
	if r.core.raft.State() != raft.Leader {
		return &NotLeaderError{Leader: string(r.core.raft.Leader())}
	}
	if err := r.core.raft.VerifyLeader().Error(); err != nil {
		return &NotLeaderError{Leader: string(r.core.raft.Leader())}
	}
	if err := r.core.raft.Barrier(r.core.readTimeout).Error(); err != nil {
		return fmt.Errorf("%w: leader read barrier: %v", queue.ErrRepositoryUnavailable, err)
	}
	return nil
}

type command struct {
	Version               uint32 `json:"version"`
	ProposalID, Operation string
	Input                 json.RawMessage    `json:"input"`
	StorageQuota          queue.StorageQuota `json:"storage_quota,omitempty"`
}
type commandResult struct {
	Value                   json.RawMessage `json:"value,omitempty"`
	ErrorCode, ErrorMessage string
}
type applyResult struct {
	result commandResult
	err    error
}

func mutate[T any](r *Repository, operation string, input any) (T, error) {
	var zero T
	encoded, err := json.Marshal(input)
	if err != nil {
		return zero, err
	}
	r.core.quotaMu.RLock()
	quota := r.core.storageQuota
	r.core.quotaMu.RUnlock()
	envelope, err := json.Marshal(command{Version: commandVersion, ProposalID: r.nextID(), Operation: operation, Input: encoded, StorageQuota: quota})
	if err != nil {
		return zero, err
	}
	future := r.core.raft.Apply(envelope, r.core.applyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return zero, &NotLeaderError{Leader: string(r.core.raft.Leader())}
		}
		return zero, fmt.Errorf("%w: replicate %s: %v", queue.ErrRepositoryUnavailable, operation, err)
	}
	applied, ok := future.Response().(applyResult)
	if !ok {
		return zero, fmt.Errorf("%w: invalid FSM response", queue.ErrRepositoryUnavailable)
	}
	if applied.err != nil {
		return zero, applied.err
	}
	if err := decodeCommandError(applied.result); err != nil {
		return zero, err
	}
	if len(applied.result.Value) > 0 {
		if err := json.Unmarshal(applied.result.Value, &zero); err != nil {
			return zero, fmt.Errorf("%w: decode replicated response", queue.ErrRepositoryCorrupt)
		}
	}
	return zero, nil
}

type fsm struct {
	stateMu         *sync.RWMutex
	local           *boltrepo.Repository
	catalogRevision string
}

func (f *fsm) Apply(entry *raft.Log) interface{} {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	var cmd command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return applyResult{err: fmt.Errorf("%w: decode command", queue.ErrRepositoryCorrupt)}
	}
	if cmd.Version != commandVersion || cmd.ProposalID == "" {
		return applyResult{err: fmt.Errorf("%w: unsupported command protocol", queue.ErrRepositoryCorrupt)}
	}
	// The leader's limit is part of the replicated command so every FSM makes
	// the same admission decision even during a rolling configuration change.
	f.local.SetStorageQuota(cmd.StorageQuota)
	result, err := f.local.ApplyReplicated(entry.Index, cmd.ProposalID, func() ([]byte, error) {
		value, opErr := dispatch(f.local, cmd)
		if opErr != nil && errors.Is(opErr, queue.ErrRepositoryUnavailable) {
			return nil, opErr
		}
		encoded, err := json.Marshal(encodeCommandResult(value, opErr))
		return encoded, err
	})
	if err != nil {
		return applyResult{err: err}
	}
	var decoded commandResult
	if err := json.Unmarshal(result.Response, &decoded); err != nil {
		return applyResult{err: fmt.Errorf("%w: decode stored command result", queue.ErrRepositoryCorrupt)}
	}
	return applyResult{result: decoded}
}

type fsmSnapshot struct{ data []byte }

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	var snapshot bytes.Buffer
	if err := f.local.WriteSnapshot(&snapshot); err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: snapshot.Bytes()}, nil
}
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, bytes.NewReader(s.data)); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *fsmSnapshot) Release() {}
func (f *fsm) Restore(reader io.ReadCloser) error {
	f.stateMu.Lock()
	defer f.stateMu.Unlock()
	defer reader.Close()
	if err := f.local.RestoreSnapshot(reader); err != nil {
		return err
	}
	if f.catalogRevision != "" {
		return f.local.BindShardCatalogRevision(f.catalogRevision)
	}
	return nil
}

func encodeCommandResult(value any, err error) commandResult {
	result := commandResult{}
	if value != nil {
		result.Value, _ = json.Marshal(value)
	}
	if err != nil {
		result.ErrorCode = errorCode(err)
		result.ErrorMessage = err.Error()
	}
	return result
}
func decodeCommandError(result commandResult) error {
	if result.ErrorCode == "" {
		return nil
	}
	base := errorForCode(result.ErrorCode)
	if base == nil {
		return errors.New(result.ErrorMessage)
	}
	return fmt.Errorf("%w: %s", base, result.ErrorMessage)
}
func errorCode(err error) string {
	for code, candidate := range knownErrors {
		if errors.Is(err, candidate) {
			return code
		}
	}
	return "operation"
}
func errorForCode(code string) error { return knownErrors[code] }

var knownErrors = map[string]error{
	"queue_exists": queue.ErrQueueAlreadyExists, "queue_missing": queue.ErrQueueDoesNotExist, "message_id_exists": queue.ErrMessageIDExists, "message_id_unavailable": queue.ErrMessageIDUnavailable, "queue_id_unavailable": queue.ErrQueueIDUnavailable, "receipt_exists": queue.ErrReceiptHandleExists, "receipt_unavailable": queue.ErrReceiptUnavailable, "receipt_invalid": queue.ErrReceiptHandleIsInvalid, "pagination": queue.ErrInvalidPaginationToken, "over_limit": queue.ErrOverLimit, "queue_in_use": queue.ErrQueueInUse, "move_running": queue.ErrMoveTaskAlreadyRunning, "move_not_running": queue.ErrMoveTaskNotRunning, "move_missing": queue.ErrMoveTaskDoesNotExist, "not_dlq": queue.ErrQueueIsNotDeadLetter, "quota": queue.ErrQuotaExceeded,
	"ownership_conflict": queue.ErrTenantOwnershipConflict, "migration_missing": queue.ErrMigrationDoesNotExist, "migration_exists": queue.ErrMigrationAlreadyExists, "tenant_migrating": queue.ErrTenantMigrating,
	"topology_missing": queue.ErrTopologyDoesNotExist, "topology_conflict": queue.ErrTopologyConflict,
}

func decodeInput[T any](raw json.RawMessage) (T, error) {
	var value T
	err := json.Unmarshal(raw, &value)
	return value, err
}
func dispatch(local *boltrepo.Repository, cmd command) (any, error) {
	switch cmd.Operation {
	case "create":
		v, e := decodeInput[queue.Queue](cmd.Input)
		if e != nil {
			return nil, e
		}
		if len(v.ID) != 34 || v.ID[:2] != "q_" {
			return nil, queue.ErrQueueIDUnavailable
		}
		return local.Create(v)
	case "delete_queue":
		v, e := decodeInput[queue.QueueRef](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.DeleteQueue(v)
	case "purge_queue":
		v, e := decodeInput[queue.QueueRef](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.PurgeQueue(v)
	case "tag_queue":
		v, e := decodeInput[struct {
			Ref  queue.QueueRef
			Tags map[string]string
		}](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.TagQueue(v.Ref, v.Tags)
	case "untag_queue":
		v, e := decodeInput[struct {
			Ref  queue.QueueRef
			Keys []string
		}](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.UntagQueue(v.Ref, v.Keys)
	case "add_permission":
		v, e := decodeInput[struct {
			Ref        queue.QueueRef
			Permission queue.Permission
		}](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.AddPermission(v.Ref, v.Permission)
	case "remove_permission":
		v, e := decodeInput[struct {
			Ref   queue.QueueRef
			Label string
		}](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.RemovePermission(v.Ref, v.Label)
	case "set_attributes":
		v, e := decodeInput[queue.QueueAttributesCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.SetAttributes(v)
	case "enqueue":
		v, e := decodeInput[queue.EnqueueCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		if v.Message.ID == "" {
			return nil, queue.ErrMessageIDUnavailable
		}
		return local.Enqueue(v)
	case "claim":
		v, e := decodeInput[queue.ClaimInput](cmd.Input)
		if e != nil {
			return nil, e
		}
		if v.MaxNumberOfMessages > len(v.ReceiptHandles) {
			return nil, queue.ErrReceiptUnavailable
		}
		return local.Claim(v)
	case "delete":
		v, e := decodeInput[struct{ Queue, ID, Receipt string }](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.Delete(v.Queue, v.ID, v.Receipt)
	case "change_visibility":
		v, e := decodeInput[queue.ChangeVisibilityCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.ChangeVisibility(v)
	case "expire":
		v, e := decodeInput[queue.ExpireCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.Expire(v)
	case "start_move":
		v, e := decodeInput[queue.StartMoveTaskCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.StartMoveTask(v)
	case "cancel_move":
		v, e := decodeInput[struct {
			Handle string
			Now    time.Time
		}](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.CancelMoveTask(v.Handle, v.Now)
	case "move_step":
		v, e := decodeInput[struct {
			Handle string
			Now    time.Time
		}](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.MoveTaskStep(v.Handle, v.Now)
	case "begin_tenant_migration":
		v, e := decodeInput[ownership.BeginCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.BeginMigration(v)
	case "transition_tenant_migration":
		v, e := decodeInput[ownership.TransitionCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.TransitionMigration(v)
	case "freeze_tenant":
		v, e := decodeInput[ownership.Migration](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.FreezeAndCapture(v)
	case "prepare_tenant":
		v, e := decodeInput[struct {
			Migration ownership.Migration
			Bundle    ownership.Bundle
		}](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.Prepare(v.Migration, v.Bundle)
	case "activate_tenant":
		v, e := decodeInput[ownership.Migration](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.Activate(v)
	case "cleanup_tenant":
		v, e := decodeInput[ownership.Migration](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.Cleanup(v)
	case "abort_tenant_source":
		v, e := decodeInput[ownership.Migration](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.AbortSource(v)
	case "abort_tenant_destination":
		v, e := decodeInput[ownership.Migration](cmd.Input)
		if e != nil {
			return nil, e
		}
		return nil, local.AbortDestination(v)
	case "initialize_topology":
		v, e := decodeInput[topology.Catalog](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.InitializeCatalog(v)
	case "ensure_tenant_assignment":
		v, e := decodeInput[string](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.EnsureAssignment(v)
	case "begin_topology_operation":
		v, e := decodeInput[topology.BeginOperationCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.BeginTopologyOperation(v)
	case "apply_topology_backfill":
		v, e := decodeInput[topology.BackfillBatchCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.ApplyBackfillBatch(v)
	case "complete_topology_backfill":
		v, e := decodeInput[string](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.CompleteBackfill(v)
	case "activate_topology_shard":
		v, e := decodeInput[string](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.ActivateShard(v)
	case "update_topology_drain":
		v, e := decodeInput[topology.DrainProgressCommand](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.UpdateDrainProgress(v)
	case "retire_topology_shard":
		v, e := decodeInput[string](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.RetireShard(v)
	case "abort_topology_operation":
		v, e := decodeInput[string](cmd.Input)
		if e != nil {
			return nil, e
		}
		return local.AbortTopologyOperation(v)
	default:
		return nil, fmt.Errorf("unknown replicated operation %q", cmd.Operation)
	}
}

func (r *Repository) Ownership(digest string) (ownership.Record, bool, error) {
	if err := r.linearizableRead(); err != nil {
		return ownership.Record{}, false, err
	}
	return r.LocalOwnership(digest)
}

func (r *Repository) LocalOwnership(digest string) (ownership.Record, bool, error) {
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.LocalOwnership(digest)
}

func (r *Repository) Migration(id string) (ownership.Migration, bool, error) {
	if err := r.linearizableRead(); err != nil {
		return ownership.Migration{}, false, err
	}
	return r.LocalMigration(id)
}

func (r *Repository) LocalMigration(id string) (ownership.Migration, bool, error) {
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.LocalMigration(id)
}

func (r *Repository) ListMigrations(limit int) ([]ownership.Migration, error) {
	if err := r.linearizableRead(); err != nil {
		return nil, err
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.ListMigrations(limit)
}

func (r *Repository) BeginMigration(command ownership.BeginCommand) (ownership.Migration, error) {
	return mutate[ownership.Migration](r, "begin_tenant_migration", command)
}

func (r *Repository) TransitionMigration(command ownership.TransitionCommand) (ownership.Migration, error) {
	return mutate[ownership.Migration](r, "transition_tenant_migration", command)
}

func (r *Repository) LocalFence(digest string) (ownership.Fence, bool, error) {
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.LocalFence(digest)
}

func (r *Repository) LocalBundle(id string) (ownership.Bundle, bool, error) {
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.LocalBundle(id)
}

func (r *Repository) FreezeAndCapture(migration ownership.Migration) (ownership.Fence, error) {
	return mutate[ownership.Fence](r, "freeze_tenant", migration)
}

func (r *Repository) Prepare(migration ownership.Migration, bundle ownership.Bundle) (ownership.Fence, error) {
	return mutate[ownership.Fence](r, "prepare_tenant", struct {
		Migration ownership.Migration
		Bundle    ownership.Bundle
	}{migration, bundle})
}

func (r *Repository) Activate(migration ownership.Migration) (ownership.Fence, error) {
	return mutate[ownership.Fence](r, "activate_tenant", migration)
}

func (r *Repository) Cleanup(migration ownership.Migration) (ownership.Fence, error) {
	return mutate[ownership.Fence](r, "cleanup_tenant", migration)
}

func (r *Repository) AbortSource(migration ownership.Migration) error {
	_, err := mutate[struct{}](r, "abort_tenant_source", migration)
	return err
}

func (r *Repository) AbortDestination(migration ownership.Migration) error {
	_, err := mutate[struct{}](r, "abort_tenant_destination", migration)
	return err
}

func (r *Repository) Catalog() (topology.Catalog, bool, error) {
	if err := r.linearizableRead(); err != nil {
		return topology.Catalog{}, false, err
	}
	return r.LocalCatalog()
}
func (r *Repository) LocalCatalog() (topology.Catalog, bool, error) {
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.LocalCatalog()
}
func (r *Repository) InitializeCatalog(v topology.Catalog) (topology.Catalog, error) {
	return mutate[topology.Catalog](r, "initialize_topology", v)
}
func (r *Repository) EnsureAssignment(v string) (ownership.Record, error) {
	return mutate[ownership.Record](r, "ensure_tenant_assignment", v)
}
func (r *Repository) OwnersByShard(shard, after string, limit int) ([]ownership.Record, bool, error) {
	if err := r.linearizableRead(); err != nil {
		return nil, false, err
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.OwnersByShard(shard, after, limit)
}
func (r *Repository) Operation(id string) (topology.Operation, bool, error) {
	if err := r.linearizableRead(); err != nil {
		return topology.Operation{}, false, err
	}
	return r.LocalOperation(id)
}
func (r *Repository) LocalOperation(id string) (topology.Operation, bool, error) {
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.LocalOperation(id)
}
func (r *Repository) ListOperations(limit int) ([]topology.Operation, error) {
	if err := r.linearizableRead(); err != nil {
		return nil, err
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.ListOperations(limit)
}
func (r *Repository) BeginTopologyOperation(v topology.BeginOperationCommand) (topology.Operation, error) {
	return mutate[topology.Operation](r, "begin_topology_operation", v)
}
func (r *Repository) ApplyBackfillBatch(v topology.BackfillBatchCommand) (topology.Operation, error) {
	return mutate[topology.Operation](r, "apply_topology_backfill", v)
}
func (r *Repository) CompleteBackfill(id string) (topology.Operation, error) {
	return mutate[topology.Operation](r, "complete_topology_backfill", id)
}
func (r *Repository) ActivateShard(id string) (topology.Operation, error) {
	return mutate[topology.Operation](r, "activate_topology_shard", id)
}
func (r *Repository) UpdateDrainProgress(v topology.DrainProgressCommand) (topology.Operation, error) {
	return mutate[topology.Operation](r, "update_topology_drain", v)
}
func (r *Repository) RetireShard(id string) (topology.Operation, error) {
	return mutate[topology.Operation](r, "retire_topology_shard", id)
}
func (r *Repository) AbortTopologyOperation(id string) (topology.Operation, error) {
	return mutate[topology.Operation](r, "abort_topology_operation", id)
}
func (r *Repository) LocalTenantDigests(after string, limit int) ([]string, bool, error) {
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.LocalTenantDigests(after, limit)
}

var _ ownership.ControlStore = (*Repository)(nil)
var _ ownership.DataStore = (*Repository)(nil)

func (r *Repository) Create(v queue.Queue) (queue.Queue, error) {
	if len(v.ID) != 34 || v.ID[:2] != "q_" {
		return queue.Queue{}, queue.ErrQueueIDUnavailable
	}
	return mutate[queue.Queue](r, "create", v)
}
func (r *Repository) Get(v string) (queue.Queue, bool, error) {
	if e := r.linearizableRead(); e != nil {
		return queue.Queue{}, false, e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.Get(v)
}
func (r *Repository) GetByRef(v queue.QueueRef) (queue.Queue, bool, error) {
	if e := r.linearizableRead(); e != nil {
		return queue.Queue{}, false, e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.GetByRef(v)
}
func (r *Repository) ListQueues(v queue.ListQueuesCommand) (queue.ListQueuesPage, error) {
	if e := r.linearizableRead(); e != nil {
		return queue.ListQueuesPage{}, e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.ListQueues(v)
}
func (r *Repository) DeleteQueue(v queue.QueueRef) error {
	_, e := mutate[struct{}](r, "delete_queue", v)
	return e
}
func (r *Repository) PurgeQueue(v queue.QueueRef) error {
	_, e := mutate[struct{}](r, "purge_queue", v)
	return e
}
func (r *Repository) TagQueue(ref queue.QueueRef, tags map[string]string) error {
	_, e := mutate[struct{}](r, "tag_queue", struct {
		Ref  queue.QueueRef
		Tags map[string]string
	}{ref, tags})
	return e
}
func (r *Repository) UntagQueue(ref queue.QueueRef, keys []string) error {
	_, e := mutate[struct{}](r, "untag_queue", struct {
		Ref  queue.QueueRef
		Keys []string
	}{ref, keys})
	return e
}
func (r *Repository) ListQueueTags(v queue.QueueRef) (map[string]string, error) {
	if e := r.linearizableRead(); e != nil {
		return nil, e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.ListQueueTags(v)
}
func (r *Repository) AddPermission(ref queue.QueueRef, p queue.Permission) error {
	_, e := mutate[struct{}](r, "add_permission", struct {
		Ref        queue.QueueRef
		Permission queue.Permission
	}{ref, p})
	return e
}
func (r *Repository) RemovePermission(ref queue.QueueRef, label string) error {
	_, e := mutate[struct{}](r, "remove_permission", struct {
		Ref   queue.QueueRef
		Label string
	}{ref, label})
	return e
}
func (r *Repository) ListPermissions(v queue.QueueRef) ([]queue.Permission, error) {
	if e := r.linearizableRead(); e != nil {
		return nil, e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.ListPermissions(v)
}
func (r *Repository) SetAttributes(v queue.QueueAttributesCommand) (queue.Queue, error) {
	return mutate[queue.Queue](r, "set_attributes", v)
}
func (r *Repository) Enqueue(v queue.EnqueueCommand) (queue.Message, error) {
	if v.Message.ID == "" {
		return queue.Message{}, queue.ErrMessageIDUnavailable
	}
	return mutate[queue.Message](r, "enqueue", v)
}
func (r *Repository) Claim(v queue.ClaimInput) (queue.ClaimResult, error) {
	if v.MaxNumberOfMessages > len(v.ReceiptHandles) {
		return queue.ClaimResult{}, queue.ErrReceiptUnavailable
	}
	return mutate[queue.ClaimResult](r, "claim", v)
}
func (r *Repository) Delete(q, id, receipt string) error {
	_, e := mutate[struct{}](r, "delete", struct{ Queue, ID, Receipt string }{q, id, receipt})
	return e
}
func (r *Repository) ChangeVisibility(v queue.ChangeVisibilityCommand) error {
	_, e := mutate[struct{}](r, "change_visibility", v)
	return e
}
func (r *Repository) Expire(v queue.ExpireCommand) (int, error) { return mutate[int](r, "expire", v) }
func (r *Repository) Health() error {
	if e := r.linearizableRead(); e != nil {
		return e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.Health()
}
func (r *Repository) ListDeadLetterSources(v queue.ListDeadLetterSourcesCommand) (queue.ListDeadLetterSourcesPage, error) {
	if e := r.linearizableRead(); e != nil {
		return queue.ListDeadLetterSourcesPage{}, e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.ListDeadLetterSources(v)
}
func (r *Repository) StartMoveTask(v queue.StartMoveTaskCommand) (queue.MessageMoveTask, error) {
	return mutate[queue.MessageMoveTask](r, "start_move", v)
}
func (r *Repository) ListMoveTasks(ref queue.QueueRef, n int) ([]queue.MessageMoveTask, error) {
	if e := r.linearizableRead(); e != nil {
		return nil, e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.ListMoveTasks(ref, n)
}
func (r *Repository) CancelMoveTask(h string, now time.Time) (queue.MessageMoveTask, error) {
	return mutate[queue.MessageMoveTask](r, "cancel_move", struct {
		Handle string
		Now    time.Time
	}{h, now})
}
func (r *Repository) MoveTaskStep(h string, now time.Time) (queue.MoveTaskStepResult, error) {
	return mutate[queue.MoveTaskStepResult](r, "move_step", struct {
		Handle string
		Now    time.Time
	}{h, now})
}
func (r *Repository) RunningMoveTasks() ([]queue.MessageMoveTask, error) {
	if e := r.linearizableRead(); e != nil {
		return nil, e
	}
	r.core.stateMu.RLock()
	defer r.core.stateMu.RUnlock()
	return r.core.local.RunningMoveTasks()
}

var _ queue.Repository = (*Repository)(nil)
var _ queue.RedriveRepository = (*Repository)(nil)
var _ queue.ClusterAdminRepository = (*Repository)(nil)
