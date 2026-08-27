package shard

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"simq/internal/ownership"
	"simq/internal/queue"
	"simq/internal/topology"
)

const topologyBatchSize = 256

func (m *Manager) topologyControl() (topology.ControlStore, error) {
	value := m.topologyStore
	if value == nil {
		return nil, queue.ErrRepositoryUnavailable
	}
	return value, nil
}

func (m *Manager) TopologyCatalog() (topology.Catalog, error) {
	if m.topologySeed == nil {
		return topology.Catalog{}, queue.ErrTopologyConflict
	}
	if catalog, found, err := m.ensureCatalog(); err != nil {
		return topology.Catalog{}, err
	} else if found {
		return catalog, nil
	}
	return topology.Catalog{}, queue.ErrTopologyConflict
}

func (m *Manager) ListTopologyOperations(limit int) ([]topology.Operation, error) {
	control, err := m.topologyControl()
	if err != nil {
		return nil, err
	}
	return control.ListOperations(limit)
}

func (m *Manager) BeginTopologyBackfill(id string) (topology.Operation, error) {
	return m.beginTopology(topology.BeginOperationCommand{OperationID: id, Kind: topology.OperationBackfill})
}
func (m *Manager) BeginShardActivation(id, shard string) (topology.Operation, error) {
	return m.beginTopology(topology.BeginOperationCommand{OperationID: id, Kind: topology.OperationActivate, ShardID: shard})
}
func (m *Manager) BeginShardDrain(id, shard string) (topology.Operation, error) {
	return m.beginTopology(topology.BeginOperationCommand{OperationID: id, Kind: topology.OperationDrain, ShardID: shard})
}
func (m *Manager) beginTopology(command topology.BeginOperationCommand) (topology.Operation, error) {
	if !topology.ValidOperationID(command.OperationID) {
		return topology.Operation{}, &queue.InvalidRequestError{Message: "topology operation ID is invalid"}
	}
	if _, err := m.TopologyCatalog(); err != nil {
		return topology.Operation{}, err
	}
	control, err := m.topologyControl()
	if err != nil {
		return topology.Operation{}, err
	}
	return control.BeginTopologyOperation(command)
}

func (m *Manager) TopologyOperationStatus(id string) (topology.Status, error) {
	if !topology.ValidOperationID(id) {
		return topology.Status{}, &queue.InvalidRequestError{Message: "topology operation ID is invalid"}
	}
	control, err := m.topologyControl()
	if err != nil {
		return topology.Status{}, err
	}
	operation, found, err := control.LocalOperation(id)
	if err != nil {
		return topology.Status{}, err
	}
	if !found {
		return topology.Status{}, queue.ErrTopologyDoesNotExist
	}
	status := topology.Status{Operation: operation}
	if operation.Phase == topology.PhaseCompleted || operation.Phase == topology.PhaseAborted {
		return status, nil
	}
	switch operation.Kind {
	case topology.OperationBackfill:
		status.NextAction, status.NextShard = "backfill", m.defaultShard
	case topology.OperationActivate:
		status.NextAction, status.NextShard = "activate", m.defaultShard
	case topology.OperationDrain:
		if operation.ActiveMigrationID != "" {
			migration, migrationErr := m.TenantMigrationStatus(operation.ActiveMigrationID)
			if migrationErr != nil {
				return status, migrationErr
			}
			if migration.Migration.Phase == ownership.PhaseCompleted {
				status.NextAction, status.NextShard = "record-move", m.defaultShard
			} else if migration.Migration.Phase == ownership.PhaseAborted {
				status.NextAction, status.NextShard = "record-abort", m.defaultShard
			} else {
				status.NextAction, status.NextShard, status.NextLeaderURL, status.LocalLeader = "advance-migration", migration.NextShard, migration.NextLeaderURL, migration.LocalLeader
				return status, nil
			}
		} else if operation.Phase == topology.PhaseAborting {
			status.NextAction, status.NextShard = "finish-abort", m.defaultShard
		} else {
			status.NextAction, status.NextShard = "evacuate-or-retire", m.defaultShard
		}
	}
	if status.NextShard != "" {
		if repository, ok := m.repositories[status.NextShard]; ok {
			if routing, ok := repository.(queue.ClusterRoutingRepository); ok {
				status.LocalLeader, status.NextLeaderURL = routing.IsLeader(), routing.LeaderAPIURL()
			} else {
				status.LocalLeader = true
			}
		} else {
			status.NextLeaderURL = m.entryURLs[status.NextShard]
		}
	}
	return status, nil
}

func (m *Manager) AdvanceTopologyOperation(id string) (topology.Status, error) {
	status, err := m.TopologyOperationStatus(id)
	if err != nil {
		return topology.Status{}, err
	}
	if status.NextAction == "" {
		return status, nil
	}
	if !status.LocalLeader {
		return status, &MigrationLeaderError{LeaderURL: status.NextLeaderURL}
	}
	control, err := m.topologyControl()
	if err != nil {
		return status, err
	}
	operation := status.Operation
	switch status.NextAction {
	case "backfill":
		if operation.ShardIndex >= len(m.initialShardIDs) {
			if _, err = control.CompleteBackfill(id); err != nil {
				return status, err
			}
			break
		}
		shardID := m.initialShardIDs[operation.ShardIndex]
		repository, ok := m.repositories[shardID]
		if !ok {
			return status, &RemoteShardError{ShardID: shardID, LeaderURL: m.entryURLs[shardID]}
		}
		data, ok := repository.(topology.DataStore)
		if !ok {
			return status, queue.ErrRepositoryUnavailable
		}
		digests, more, scanErr := data.LocalTenantDigests(operation.AfterDigest, topologyBatchSize)
		if scanErr != nil {
			return status, scanErr
		}
		records := make([]ownership.Record, len(digests))
		for index, digest := range digests {
			records[index] = ownership.Record{Version: ownership.RecordVersion, TenantDigest: digest, ShardID: shardID, Epoch: 1}
		}
		nextIndex, nextAfter := operation.ShardIndex+1, ""
		if more {
			nextIndex = operation.ShardIndex
			nextAfter = digests[len(digests)-1]
		}
		if _, err = control.ApplyBackfillBatch(topology.BackfillBatchCommand{OperationID: id, ShardIndex: operation.ShardIndex, AfterDigest: operation.AfterDigest, Records: records, NextShardIndex: nextIndex, NextAfterDigest: nextAfter}); err != nil {
			return status, err
		}
	case "activate":
		if _, err = control.ActivateShard(id); err != nil {
			return status, err
		}
	case "advance-migration":
		if _, err = m.AdvanceTenantMigration(operation.ActiveMigrationID); err != nil {
			return status, err
		}
	case "record-move":
		if _, err = control.UpdateDrainProgress(topology.DrainProgressCommand{OperationID: id, ExpectedMigrationID: operation.ActiveMigrationID, IncrementMoved: true}); err != nil {
			return status, err
		}
	case "record-abort":
		if _, err = control.UpdateDrainProgress(topology.DrainProgressCommand{OperationID: id, ExpectedMigrationID: operation.ActiveMigrationID}); err != nil {
			return status, err
		}
	case "finish-abort":
		if _, err = control.AbortTopologyOperation(id); err != nil {
			return status, err
		}
	case "evacuate-or-retire":
		owners, _, scanErr := control.OwnersByShard(operation.ShardID, "", 1)
		if scanErr != nil {
			return status, scanErr
		}
		if len(owners) == 0 {
			if _, err = control.RetireShard(id); err != nil {
				return status, err
			}
			break
		}
		catalog, catalogErr := m.TopologyCatalog()
		if catalogErr != nil {
			return status, catalogErr
		}
		destination, selectErr := topology.SelectShard(owners[0].TenantDigest, catalog, false, operation.ShardID)
		if selectErr != nil {
			return status, queue.ErrTopologyConflict
		}
		migrationID := topologyMigrationID(id, owners[0].TenantDigest)
		migration, beginErr := m.BeginTenantMigration(migrationID, owners[0].TenantDigest, destination)
		if beginErr != nil && !errors.Is(beginErr, queue.ErrMigrationAlreadyExists) {
			return status, beginErr
		}
		if beginErr != nil {
			ownershipControl, controlErr := m.controlStore()
			if controlErr != nil {
				return status, controlErr
			}
			var found bool
			migration, found, beginErr = ownershipControl.Migration(migrationID)
			if beginErr != nil || !found || migration.TenantDigest != owners[0].TenantDigest {
				return status, queue.ErrTopologyConflict
			}
		}
		if _, err = control.UpdateDrainProgress(topology.DrainProgressCommand{OperationID: id, NextMigrationID: migrationID}); err != nil {
			return status, err
		}
	default:
		return status, fmt.Errorf("unknown topology action %q", status.NextAction)
	}
	return m.TopologyOperationStatus(id)
}

func (m *Manager) AbortTopologyOperation(id string) (topology.Status, error) {
	status, err := m.TopologyOperationStatus(id)
	if err != nil {
		return topology.Status{}, err
	}
	if status.Operation.Phase == topology.PhaseCompleted || status.Operation.Phase == topology.PhaseAborted {
		return status, nil
	}
	control, err := m.topologyControl()
	if err != nil {
		return status, err
	}
	operation, err := control.AbortTopologyOperation(id)
	if err != nil {
		return status, err
	}
	if operation.Kind == topology.OperationDrain && operation.ActiveMigrationID != "" {
		if _, err := m.AbortTenantMigration(operation.ActiveMigrationID); err != nil && !errors.Is(err, queue.ErrTenantOwnershipConflict) {
			return status, err
		}
	}
	return m.TopologyOperationStatus(id)
}

func topologyMigrationID(operationID, digest string) string {
	sum := sha256.Sum256([]byte(operationID + "\x00" + digest))
	return "tm_" + hex.EncodeToString(sum[:16])
}

var _ queue.TopologyAdminRepository = (*Manager)(nil)
