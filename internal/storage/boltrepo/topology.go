package boltrepo

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"simq/internal/ownership"
	"simq/internal/queue"
	"simq/internal/topology"
)

var topologyCatalogKey = []byte("catalog")

func (r *Repository) Catalog() (topology.Catalog, bool, error) { return r.LocalCatalog() }

func (r *Repository) LocalCatalog() (topology.Catalog, bool, error) {
	var result topology.Catalog
	var found bool
	err := r.view(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(topologyCatalogBucket).Get(topologyCatalogKey)
		if encoded == nil {
			return nil
		}
		if err := decodeRecord(encoded, &result); err != nil {
			return err
		}
		found = true
		return result.Canonicalize()
	})
	return result, found, wrapOperationError("read topology catalog", err)
}

func (r *Repository) InitializeCatalog(input topology.Catalog) (topology.Catalog, error) {
	if err := input.Canonicalize(); err != nil {
		return topology.Catalog{}, &queue.InvalidRequestError{Message: err.Error()}
	}
	err := r.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(topologyCatalogBucket)
		current := bucket.Get(topologyCatalogKey)
		encoded, err := encodeRecord(input)
		if err != nil {
			return err
		}
		if current != nil {
			if bytes.Equal(current, encoded) {
				return nil
			}
			return queue.ErrTopologyConflict
		}
		return bucket.Put(topologyCatalogKey, encoded)
	})
	return input, wrapOperationError("initialize topology catalog", err)
}

func (r *Repository) EnsureAssignment(digest string) (ownership.Record, error) {
	if !ownership.ValidDigest(digest) {
		return ownership.Record{}, &queue.InvalidRequestError{Message: "tenant digest is invalid"}
	}
	var result ownership.Record
	err := r.update(func(tx *bolt.Tx) error {
		owners := tx.Bucket(tenantOwnershipBucket)
		if encoded := owners.Get([]byte(digest)); encoded != nil {
			if err := decodeRecord(encoded, &result); err != nil {
				return err
			}
			return result.Validate()
		}
		catalog, found, err := readTopologyCatalog(tx)
		if err != nil {
			return err
		}
		if !found {
			return queue.ErrTopologyConflict
		}
		shardID, err := topology.SelectShard(digest, catalog, catalog.DirectoryMode != topology.DirectoryExplicit, "")
		if err != nil {
			return queue.ErrTopologyConflict
		}
		result = ownership.Record{Version: ownership.RecordVersion, TenantDigest: digest, ShardID: shardID, Epoch: 1}
		return putRecord(owners, digest, result)
	})
	return result, wrapOperationError("assign tenant shard", err)
}

func (r *Repository) OwnersByShard(shardID, after string, limit int) ([]ownership.Record, bool, error) {
	if !topology.ValidShardID(shardID) || after != "" && !ownership.ValidDigest(after) || limit < 1 || limit > 1000 {
		return nil, false, &queue.InvalidRequestError{Message: "owner scan input is invalid"}
	}
	result := make([]ownership.Record, 0, limit)
	more := false
	err := r.view(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(tenantOwnershipBucket).Cursor()
		key, value := cursor.First()
		if after != "" {
			key, value = cursor.Seek([]byte(after))
			if string(key) == after {
				key, value = cursor.Next()
			}
		}
		for ; key != nil; key, value = cursor.Next() {
			var record ownership.Record
			if err := decodeRecord(value, &record); err != nil {
				return err
			}
			if record.ShardID != shardID {
				continue
			}
			if len(result) == limit {
				more = true
				break
			}
			result = append(result, record)
		}
		return nil
	})
	return result, more, wrapOperationError("list tenant owners", err)
}

func (r *Repository) Operation(id string) (topology.Operation, bool, error) {
	return r.LocalOperation(id)
}

func (r *Repository) LocalOperation(id string) (topology.Operation, bool, error) {
	var result topology.Operation
	var found bool
	err := r.view(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(topologyOperationsBucket).Get([]byte(id))
		if encoded == nil {
			return nil
		}
		if err := decodeRecord(encoded, &result); err != nil {
			return err
		}
		found = true
		return result.Validate()
	})
	return result, found, wrapOperationError("read topology operation", err)
}

func (r *Repository) ListOperations(limit int) ([]topology.Operation, error) {
	if limit < 1 || limit > 1000 {
		return nil, &queue.InvalidRequestError{Message: "topology operation list limit must be from 1 through 1000"}
	}
	result := make([]topology.Operation, 0, limit)
	err := r.view(func(tx *bolt.Tx) error {
		return tx.Bucket(topologyOperationsBucket).ForEach(func(_, encoded []byte) error {
			if len(result) == limit {
				return nil
			}
			var operation topology.Operation
			if err := decodeRecord(encoded, &operation); err != nil {
				return err
			}
			result = append(result, operation)
			return nil
		})
	})
	return result, wrapOperationError("list topology operations", err)
}

func (r *Repository) BeginTopologyOperation(command topology.BeginOperationCommand) (topology.Operation, error) {
	var result topology.Operation
	if !topology.ValidOperationID(command.OperationID) {
		return result, &queue.InvalidRequestError{Message: "topology operation ID is invalid"}
	}
	err := r.update(func(tx *bolt.Tx) error {
		operations := tx.Bucket(topologyOperationsBucket)
		if encoded := operations.Get([]byte(command.OperationID)); encoded != nil {
			if err := decodeRecord(encoded, &result); err != nil {
				return err
			}
			if result.Kind == command.Kind && result.ShardID == command.ShardID {
				return nil
			}
			return queue.ErrTopologyConflict
		}
		catalog, found, err := readTopologyCatalog(tx)
		if err != nil {
			return err
		}
		if !found {
			return queue.ErrTopologyConflict
		}
		result = topology.Operation{Version: topology.RecordVersion, ID: command.OperationID, Kind: command.Kind, ShardID: command.ShardID, Generation: catalog.Generation}
		switch command.Kind {
		case topology.OperationBackfill:
			if command.ShardID != "" || catalog.DirectoryMode != topology.DirectoryLegacy {
				return queue.ErrTopologyConflict
			}
			result.Phase = topology.PhaseRunning
			catalog.DirectoryMode = topology.DirectoryBackfilling
			catalog.Generation++
			result.Generation = catalog.Generation
			if err := writeTopologyCatalog(tx, catalog); err != nil {
				return err
			}
		case topology.OperationActivate:
			shard, ok := topology.FindShard(catalog, command.ShardID)
			if !ok || shard.State != topology.ShardCandidate || !catalog.BackfillComplete {
				return queue.ErrTopologyConflict
			}
			if tx.Bucket(topologyTombstonesBucket).Get([]byte(command.ShardID)) != nil {
				return queue.ErrTopologyConflict
			}
			result.Phase = topology.PhasePlanned
		case topology.OperationDrain:
			shard, ok := topology.FindShard(catalog, command.ShardID)
			if !ok || shard.ID == catalog.DefaultShard || shard.State != topology.ShardReady || !catalog.BackfillComplete {
				return queue.ErrTopologyConflict
			}
			result.Phase = topology.PhaseRunning
			if err := setShardState(&catalog, command.ShardID, topology.ShardDraining); err != nil {
				return err
			}
			catalog.Generation++
			result.Generation = catalog.Generation
			if err := writeTopologyCatalog(tx, catalog); err != nil {
				return err
			}
		default:
			return &queue.InvalidRequestError{Message: "topology operation kind is invalid"}
		}
		return putRecord(operations, result.ID, result)
	})
	return result, wrapOperationError("begin topology operation", err)
}

func (r *Repository) ApplyBackfillBatch(command topology.BackfillBatchCommand) (topology.Operation, error) {
	var result topology.Operation
	err := r.update(func(tx *bolt.Tx) error {
		operation, err := readTopologyOperation(tx, command.OperationID)
		if err != nil {
			return err
		}
		if operation.Kind != topology.OperationBackfill || operation.Phase != topology.PhaseRunning || operation.ShardIndex != command.ShardIndex || operation.AfterDigest != command.AfterDigest {
			return queue.ErrTopologyConflict
		}
		owners := tx.Bucket(tenantOwnershipBucket)
		for _, record := range command.Records {
			if record.Validate() != nil || record.Epoch != 1 {
				return queue.ErrTopologyConflict
			}
			if encoded := owners.Get([]byte(record.TenantDigest)); encoded != nil {
				var existing ownership.Record
				if decodeRecord(encoded, &existing) != nil || existing.ShardID != record.ShardID {
					return queue.ErrTopologyConflict
				}
				continue
			}
			if err := putRecord(owners, record.TenantDigest, record); err != nil {
				return err
			}
		}
		operation.ShardIndex, operation.AfterDigest = command.NextShardIndex, command.NextAfterDigest
		result = operation
		return putRecord(tx.Bucket(topologyOperationsBucket), operation.ID, operation)
	})
	return result, wrapOperationError("apply topology backfill batch", err)
}

func (r *Repository) CompleteBackfill(id string) (topology.Operation, error) {
	return r.finishTopology(id, func(tx *bolt.Tx, catalog *topology.Catalog, operation *topology.Operation) error {
		if operation.Kind != topology.OperationBackfill || operation.Phase != topology.PhaseRunning || catalog.DirectoryMode != topology.DirectoryBackfilling {
			return queue.ErrTopologyConflict
		}
		catalog.DirectoryMode, catalog.BackfillComplete = topology.DirectoryExplicit, true
		catalog.Generation++
		operation.Generation, operation.Phase = catalog.Generation, topology.PhaseCompleted
		return nil
	})
}

func (r *Repository) ActivateShard(id string) (topology.Operation, error) {
	return r.finishTopology(id, func(tx *bolt.Tx, catalog *topology.Catalog, operation *topology.Operation) error {
		if operation.Kind != topology.OperationActivate || operation.Phase != topology.PhasePlanned || !catalog.BackfillComplete {
			return queue.ErrTopologyConflict
		}
		if tx.Bucket(topologyTombstonesBucket).Get([]byte(operation.ShardID)) != nil {
			return queue.ErrTopologyConflict
		}
		if err := setShardState(catalog, operation.ShardID, topology.ShardReady); err != nil {
			return err
		}
		catalog.Generation++
		operation.Generation, operation.Phase = catalog.Generation, topology.PhaseCompleted
		return nil
	})
}

func (r *Repository) UpdateDrainProgress(command topology.DrainProgressCommand) (topology.Operation, error) {
	var result topology.Operation
	err := r.update(func(tx *bolt.Tx) error {
		operation, err := readTopologyOperation(tx, command.OperationID)
		if err != nil {
			return err
		}
		if operation.Kind != topology.OperationDrain || operation.Phase != topology.PhaseRunning && operation.Phase != topology.PhaseAborting || operation.ActiveMigrationID != command.ExpectedMigrationID {
			return queue.ErrTopologyConflict
		}
		if command.NextMigrationID != "" && !ownership.ValidMigrationID(command.NextMigrationID) {
			return &queue.InvalidRequestError{Message: "migration ID is invalid"}
		}
		operation.ActiveMigrationID = command.NextMigrationID
		if command.IncrementMoved {
			operation.MovedCount++
		}
		result = operation
		return putRecord(tx.Bucket(topologyOperationsBucket), operation.ID, operation)
	})
	return result, wrapOperationError("update drain progress", err)
}

func (r *Repository) RetireShard(id string) (topology.Operation, error) {
	return r.finishTopology(id, func(tx *bolt.Tx, catalog *topology.Catalog, operation *topology.Operation) error {
		if operation.Kind != topology.OperationDrain || operation.Phase != topology.PhaseRunning || operation.ActiveMigrationID != "" {
			return queue.ErrTopologyConflict
		}
		owned, err := hasShardOwner(tx, operation.ShardID)
		if err != nil {
			return err
		}
		if owned {
			return queue.ErrTopologyConflict
		}
		shard, ok := topology.FindShard(*catalog, operation.ShardID)
		if !ok || shard.State != topology.ShardDraining {
			return queue.ErrTopologyConflict
		}
		if err := setShardState(catalog, operation.ShardID, topology.ShardRetired); err != nil {
			return err
		}
		catalog.Generation++
		operation.Generation, operation.Phase = catalog.Generation, topology.PhaseCompleted
		tombstone := topology.Tombstone{Version: topology.RecordVersion, Generation: catalog.Generation, ShardID: shard.ID, Incarnation: shard.Incarnation}
		return putRecord(tx.Bucket(topologyTombstonesBucket), shard.ID, tombstone)
	})
}

func (r *Repository) AbortTopologyOperation(id string) (topology.Operation, error) {
	return r.finishTopology(id, func(tx *bolt.Tx, catalog *topology.Catalog, operation *topology.Operation) error {
		switch operation.Kind {
		case topology.OperationActivate:
			if operation.Phase != topology.PhasePlanned {
				return queue.ErrTopologyConflict
			}
		case topology.OperationDrain:
			if operation.MovedCount != 0 || operation.Phase != topology.PhaseRunning && operation.Phase != topology.PhaseAborting {
				return queue.ErrTopologyConflict
			}
			if operation.ActiveMigrationID != "" {
				encoded := tx.Bucket(tenantMigrationsBucket).Get([]byte(operation.ActiveMigrationID))
				var migration ownership.Migration
				if encoded == nil || decodeRecord(encoded, &migration) != nil || migration.Phase != ownership.PhaseFreezing && migration.Phase != ownership.PhasePreparing && migration.Phase != ownership.PhaseCuttingOver {
					return queue.ErrTopologyConflict
				}
				operation.Phase = topology.PhaseAborting
				return nil
			}
			if err := setShardState(catalog, operation.ShardID, topology.ShardReady); err != nil {
				return err
			}
			catalog.Generation++
			operation.Generation = catalog.Generation
		default:
			return queue.ErrTopologyConflict
		}
		operation.Phase = topology.PhaseAborted
		return nil
	})
}

func (r *Repository) LocalTenantDigests(after string, limit int) ([]string, bool, error) {
	if after != "" && !ownership.ValidDigest(after) || limit < 1 || limit > 1000 {
		return nil, false, &queue.InvalidRequestError{Message: "tenant scan input is invalid"}
	}
	result := make([]string, 0, limit)
	more := false
	err := r.view(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(tenantUsageBucket).Cursor()
		key, _ := cursor.First()
		if after != "" {
			key, _ = cursor.Seek([]byte(after))
			if string(key) == after {
				key, _ = cursor.Next()
			}
		}
		for ; key != nil; key, _ = cursor.Next() {
			if !ownership.ValidDigest(string(key)) {
				continue
			}
			if len(result) == limit {
				more = true
				break
			}
			result = append(result, string(key))
		}
		return nil
	})
	return result, more, wrapOperationError("scan local tenant digests", err)
}

func (r *Repository) finishTopology(id string, transition func(*bolt.Tx, *topology.Catalog, *topology.Operation) error) (topology.Operation, error) {
	var result topology.Operation
	err := r.update(func(tx *bolt.Tx) error {
		operation, err := readTopologyOperation(tx, id)
		if err != nil {
			return err
		}
		if operation.Phase == topology.PhaseCompleted || operation.Phase == topology.PhaseAborted {
			result = operation
			return nil
		}
		catalog, found, err := readTopologyCatalog(tx)
		if err != nil {
			return err
		}
		if !found {
			return queue.ErrTopologyConflict
		}
		if err := transition(tx, &catalog, &operation); err != nil {
			return err
		}
		if err := writeTopologyCatalog(tx, catalog); err != nil {
			return err
		}
		if err := putRecord(tx.Bucket(topologyOperationsBucket), operation.ID, operation); err != nil {
			return err
		}
		result = operation
		return nil
	})
	return result, wrapOperationError("transition topology operation", err)
}

func readTopologyCatalog(tx *bolt.Tx) (topology.Catalog, bool, error) {
	var result topology.Catalog
	encoded := tx.Bucket(topologyCatalogBucket).Get(topologyCatalogKey)
	if encoded == nil {
		return result, false, nil
	}
	if err := decodeRecord(encoded, &result); err != nil {
		return result, false, err
	}
	if err := result.Canonicalize(); err != nil {
		return result, false, corruptf("topology catalog is invalid")
	}
	return result, true, nil
}

func writeTopologyCatalog(tx *bolt.Tx, catalog topology.Catalog) error {
	if err := catalog.Canonicalize(); err != nil {
		return err
	}
	return putRecord(tx.Bucket(topologyCatalogBucket), string(topologyCatalogKey), catalog)
}

func readTopologyOperation(tx *bolt.Tx, id string) (topology.Operation, error) {
	var result topology.Operation
	encoded := tx.Bucket(topologyOperationsBucket).Get([]byte(id))
	if encoded == nil {
		return result, queue.ErrTopologyDoesNotExist
	}
	if err := decodeRecord(encoded, &result); err != nil {
		return result, err
	}
	if err := result.Validate(); err != nil {
		return result, corruptf("topology operation is invalid")
	}
	return result, nil
}

func setShardState(catalog *topology.Catalog, id string, state topology.ShardState) error {
	for index := range catalog.Shards {
		if catalog.Shards[index].ID == id {
			catalog.Shards[index].State = state
			return nil
		}
	}
	return fmt.Errorf("topology shard is missing")
}

func hasShardOwner(tx *bolt.Tx, shardID string) (bool, error) {
	found := false
	err := tx.Bucket(tenantOwnershipBucket).ForEach(func(_, encoded []byte) error {
		var record ownership.Record
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		if record.ShardID == shardID {
			found = true
		}
		return nil
	})
	return found, err
}
