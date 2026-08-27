package boltrepo

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"simq/internal/ownership"
	"simq/internal/queue"
)

var tenantNamedBuckets = [][]byte{
	queuesBucket, queueIdentitiesBucket, queueTagsBucket, queuePermissionsBucket,
	queueTombstonesBucket, redrivePoliciesBucket, fifoQueuesBucket,
	fifoSequencesBucket,
}

var tenantMessageBuckets = [][]byte{
	messagesBucket, messageIDsBucket, redriveOriginsBucket, fifoMessagesBucket,
}

func (r *Repository) Ownership(digest string) (ownership.Record, bool, error) {
	return r.LocalOwnership(digest)
}

func (r *Repository) LocalOwnership(digest string) (ownership.Record, bool, error) {
	var result ownership.Record
	var found bool
	err := r.view(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(tenantOwnershipBucket).Get([]byte(digest))
		if encoded == nil {
			return nil
		}
		if err := decodeRecord(encoded, &result); err != nil {
			return err
		}
		found = true
		return result.Validate()
	})
	return result, found, wrapOperationError("read tenant ownership", err)
}

func (r *Repository) Migration(id string) (ownership.Migration, bool, error) {
	return r.LocalMigration(id)
}

func (r *Repository) LocalMigration(id string) (ownership.Migration, bool, error) {
	var result ownership.Migration
	var found bool
	err := r.view(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(tenantMigrationsBucket).Get([]byte(id))
		if encoded == nil {
			return nil
		}
		if err := decodeRecord(encoded, &result); err != nil {
			return err
		}
		found = true
		return result.Validate()
	})
	return result, found, wrapOperationError("read tenant migration", err)
}

func (r *Repository) ListMigrations(limit int) ([]ownership.Migration, error) {
	if limit < 1 || limit > 1000 {
		return nil, &queue.InvalidRequestError{Message: "migration list limit must be from 1 through 1000"}
	}
	result := make([]ownership.Migration, 0, limit)
	err := r.view(func(tx *bolt.Tx) error {
		return tx.Bucket(tenantMigrationsBucket).ForEach(func(_, encoded []byte) error {
			if len(result) == limit {
				return nil
			}
			var item ownership.Migration
			if err := decodeRecord(encoded, &item); err != nil {
				return err
			}
			if err := item.Validate(); err != nil {
				return corruptf("tenant migration record is invalid")
			}
			result = append(result, item)
			return nil
		})
	})
	return result, wrapOperationError("list tenant migrations", err)
}

func (r *Repository) BeginMigration(command ownership.BeginCommand) (ownership.Migration, error) {
	result := ownership.Migration{Version: ownership.RecordVersion, ID: command.MigrationID, TenantDigest: command.TenantDigest, SourceShard: command.SourceShard, DestinationShard: command.DestinationShard, SourceEpoch: command.SourceEpoch, DestinationEpoch: command.SourceEpoch + 1, Phase: ownership.PhaseFreezing}
	if err := result.Validate(); err != nil {
		return ownership.Migration{}, &queue.InvalidRequestError{Message: "tenant migration input is invalid"}
	}
	err := r.update(func(tx *bolt.Tx) error {
		migrations := tx.Bucket(tenantMigrationsBucket)
		if migrations.Get([]byte(result.ID)) != nil {
			return queue.ErrMigrationAlreadyExists
		}
		if err := migrations.ForEach(func(_, encoded []byte) error {
			var existing ownership.Migration
			if err := decodeRecord(encoded, &existing); err != nil {
				return err
			}
			if existing.TenantDigest == result.TenantDigest && existing.Phase != ownership.PhaseCompleted && existing.Phase != ownership.PhaseAborted {
				return queue.ErrMigrationAlreadyExists
			}
			return nil
		}); err != nil {
			return err
		}
		owners := tx.Bucket(tenantOwnershipBucket)
		if encoded := owners.Get([]byte(result.TenantDigest)); encoded != nil {
			var current ownership.Record
			if err := decodeRecord(encoded, &current); err != nil {
				return err
			}
			if current.ShardID != result.SourceShard || current.Epoch != result.SourceEpoch {
				return queue.ErrTenantOwnershipConflict
			}
		} else {
			if result.SourceEpoch != 1 {
				return queue.ErrTenantOwnershipConflict
			}
			encoded, err := encodeRecord(ownership.Record{Version: ownership.RecordVersion, TenantDigest: result.TenantDigest, ShardID: result.SourceShard, Epoch: result.SourceEpoch})
			if err != nil {
				return err
			}
			if err := owners.Put([]byte(result.TenantDigest), encoded); err != nil {
				return err
			}
		}
		encoded, err := encodeRecord(result)
		if err != nil {
			return err
		}
		return migrations.Put([]byte(result.ID), encoded)
	})
	return result, wrapOperationError("begin tenant migration", err)
}

func (r *Repository) TransitionMigration(command ownership.TransitionCommand) (ownership.Migration, error) {
	var result ownership.Migration
	err := r.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(tenantMigrationsBucket)
		encoded := bucket.Get([]byte(command.MigrationID))
		if encoded == nil {
			return queue.ErrMigrationDoesNotExist
		}
		if err := decodeRecord(encoded, &result); err != nil {
			return err
		}
		if result.Phase == command.NextPhase {
			return nil
		}
		if result.Phase != command.ExpectedPhase || !validMigrationTransition(result.Phase, command.NextPhase) {
			return queue.ErrTenantOwnershipConflict
		}
		if command.BundleHash != "" {
			if !ownership.ValidHash(command.BundleHash) || result.BundleHash != "" && result.BundleHash != command.BundleHash {
				return queue.ErrTenantOwnershipConflict
			}
			result.BundleHash = command.BundleHash
		}
		if command.NextPhase != ownership.PhaseAborting && command.NextPhase != ownership.PhaseAborted && result.Phase != ownership.PhaseFreezing && result.BundleHash == "" {
			return queue.ErrTenantOwnershipConflict
		}
		if command.NextPhase == ownership.PhaseActivating {
			owners := tx.Bucket(tenantOwnershipBucket)
			currentEncoded := owners.Get([]byte(result.TenantDigest))
			var current ownership.Record
			if currentEncoded == nil || decodeRecord(currentEncoded, &current) != nil || current.ShardID != result.SourceShard || current.Epoch != result.SourceEpoch {
				return queue.ErrTenantOwnershipConflict
			}
			next := ownership.Record{Version: ownership.RecordVersion, TenantDigest: result.TenantDigest, ShardID: result.DestinationShard, Epoch: result.DestinationEpoch}
			nextEncoded, err := encodeRecord(next)
			if err != nil {
				return err
			}
			if err := owners.Put([]byte(result.TenantDigest), nextEncoded); err != nil {
				return err
			}
		}
		result.Phase = command.NextPhase
		updated, err := encodeRecord(result)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(result.ID), updated)
	})
	return result, wrapOperationError("transition tenant migration", err)
}

func validMigrationTransition(current, next ownership.Phase) bool {
	if next == ownership.PhaseAborting {
		return current == ownership.PhaseFreezing || current == ownership.PhasePreparing || current == ownership.PhaseCuttingOver
	}
	switch current {
	case ownership.PhaseFreezing:
		return next == ownership.PhasePreparing
	case ownership.PhasePreparing:
		return next == ownership.PhaseCuttingOver
	case ownership.PhaseCuttingOver:
		return next == ownership.PhaseActivating
	case ownership.PhaseActivating:
		return next == ownership.PhaseCleaning
	case ownership.PhaseCleaning:
		return next == ownership.PhaseCompleted
	case ownership.PhaseAborting:
		return next == ownership.PhaseAborted
	default:
		return false
	}
}

func (r *Repository) LocalFence(digest string) (ownership.Fence, bool, error) {
	var result ownership.Fence
	var found bool
	err := r.view(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(tenantFencesBucket).Get([]byte(digest))
		if encoded == nil {
			return nil
		}
		if err := decodeRecord(encoded, &result); err != nil {
			return err
		}
		found = true
		return result.Validate()
	})
	return result, found, wrapOperationError("read tenant fence", err)
}

func (r *Repository) LocalBundle(id string) (ownership.Bundle, bool, error) {
	var result ownership.Bundle
	var found bool
	err := r.view(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(tenantBundlesBucket).Get([]byte(id))
		if encoded == nil {
			return nil
		}
		if err := decodeRecord(encoded, &result); err != nil {
			return err
		}
		found = true
		return ownership.CanonicalizeBundle(&result)
	})
	return result, found, wrapOperationError("read tenant bundle", err)
}

func (r *Repository) FreezeAndCapture(migration ownership.Migration) (ownership.Fence, error) {
	var result ownership.Fence
	if migration.Phase != ownership.PhaseFreezing || migration.Validate() != nil {
		return result, queue.ErrTenantOwnershipConflict
	}
	err := r.update(func(tx *bolt.Tx) error {
		if existing, found, err := readFence(tx, migration.TenantDigest); err != nil {
			return err
		} else if found {
			if existing.MigrationID == migration.ID && existing.State == ownership.FenceFrozen && existing.Epoch == migration.SourceEpoch {
				result = existing
				return nil
			}
			if existing.State != ownership.FenceActive || existing.Epoch != migration.SourceEpoch {
				return queue.ErrTenantOwnershipConflict
			}
		}
		if runningTenantMoveTask(tx, migration.TenantDigest) {
			return queue.ErrQueueInUse
		}
		bundle, err := captureTenantBundle(tx, migration)
		if err != nil {
			return err
		}
		result = ownership.Fence{Version: ownership.RecordVersion, MigrationID: migration.ID, TenantDigest: migration.TenantDigest, Epoch: migration.SourceEpoch, State: ownership.FenceFrozen, DestinationShard: migration.DestinationShard, BundleHash: bundle.Hash}
		if err := putRecord(tx.Bucket(tenantBundlesBucket), migration.ID, bundle); err != nil {
			return err
		}
		return putRecord(tx.Bucket(tenantFencesBucket), migration.TenantDigest, result)
	})
	return result, wrapOperationError("freeze and capture tenant", err)
}

func (r *Repository) Prepare(migration ownership.Migration, bundle ownership.Bundle) (ownership.Fence, error) {
	var result ownership.Fence
	if migration.Phase != ownership.PhasePreparing || migration.Validate() != nil || ownership.CanonicalizeBundle(&bundle) != nil || bundle.MigrationID != migration.ID || bundle.TenantDigest != migration.TenantDigest || bundle.SourceEpoch != migration.SourceEpoch || bundle.Hash != migration.BundleHash {
		return result, queue.ErrTenantOwnershipConflict
	}
	err := r.update(func(tx *bolt.Tx) error {
		if existing, found, err := readFence(tx, migration.TenantDigest); err != nil {
			return err
		} else if found {
			if existing.MigrationID == migration.ID && existing.State == ownership.FencePrepared && existing.BundleHash == bundle.Hash {
				result = existing
				return nil
			}
			// A completed move deliberately leaves a MOVED fence behind. A later
			// migration may return the tenant to this shard once cleanup proved
			// that no tenant data remains here.
			if existing.State != ownership.FenceMoved || tenantHasData(tx, migration.TenantDigest) {
				return queue.ErrTenantOwnershipConflict
			}
		}
		if tenantHasData(tx, migration.TenantDigest) {
			return queue.ErrTenantOwnershipConflict
		}
		if err := applyTenantBundle(tx, bundle); err != nil {
			return err
		}
		result = ownership.Fence{Version: ownership.RecordVersion, MigrationID: migration.ID, TenantDigest: migration.TenantDigest, Epoch: migration.DestinationEpoch, State: ownership.FencePrepared, BundleHash: bundle.Hash}
		if err := putRecord(tx.Bucket(tenantBundlesBucket), migration.ID, bundle); err != nil {
			return err
		}
		return putRecord(tx.Bucket(tenantFencesBucket), migration.TenantDigest, result)
	})
	return result, wrapOperationError("prepare tenant", err)
}

func (r *Repository) Activate(migration ownership.Migration) (ownership.Fence, error) {
	var result ownership.Fence
	if migration.Phase != ownership.PhaseActivating {
		return result, queue.ErrTenantOwnershipConflict
	}
	err := r.update(func(tx *bolt.Tx) error {
		fence, found, err := readFence(tx, migration.TenantDigest)
		if err != nil {
			return err
		}
		if !found || fence.MigrationID != migration.ID || fence.Epoch != migration.DestinationEpoch || fence.BundleHash != migration.BundleHash || fence.State != ownership.FencePrepared && fence.State != ownership.FenceActive {
			return queue.ErrTenantOwnershipConflict
		}
		fence.State = ownership.FenceActive
		result = fence
		return putRecord(tx.Bucket(tenantFencesBucket), migration.TenantDigest, fence)
	})
	return result, wrapOperationError("activate tenant", err)
}

func (r *Repository) Cleanup(migration ownership.Migration) (ownership.Fence, error) {
	var result ownership.Fence
	if migration.Phase != ownership.PhaseCleaning {
		return result, queue.ErrTenantOwnershipConflict
	}
	err := r.update(func(tx *bolt.Tx) error {
		fence, found, err := readFence(tx, migration.TenantDigest)
		if err != nil {
			return err
		}
		if found && fence.State == ownership.FenceMoved && fence.MigrationID == migration.ID {
			result = fence
			return nil
		}
		if !found || fence.State != ownership.FenceFrozen || fence.MigrationID != migration.ID || fence.BundleHash != migration.BundleHash {
			return queue.ErrTenantOwnershipConflict
		}
		bundle, found, err := readBundle(tx, migration.ID)
		if err != nil || !found {
			return firstError(err, queue.ErrTenantOwnershipConflict)
		}
		if err := deleteTenantBundleEntries(tx, bundle); err != nil {
			return err
		}
		result = ownership.Fence{Version: ownership.RecordVersion, MigrationID: migration.ID, TenantDigest: migration.TenantDigest, Epoch: migration.SourceEpoch, State: ownership.FenceMoved, DestinationShard: migration.DestinationShard, BundleHash: migration.BundleHash}
		if err := tx.Bucket(tenantBundlesBucket).Delete([]byte(migration.ID)); err != nil {
			return err
		}
		return putRecord(tx.Bucket(tenantFencesBucket), migration.TenantDigest, result)
	})
	return result, wrapOperationError("clean tenant source", err)
}

func (r *Repository) AbortSource(migration ownership.Migration) error {
	if migration.Phase != ownership.PhaseAborting && migration.Phase != ownership.PhaseAborted {
		return queue.ErrTenantOwnershipConflict
	}
	return r.update(func(tx *bolt.Tx) error {
		fence, found, err := readFence(tx, migration.TenantDigest)
		if err != nil || !found {
			return err
		}
		if fence.MigrationID != migration.ID || fence.State != ownership.FenceFrozen {
			return queue.ErrTenantOwnershipConflict
		}
		if err := tx.Bucket(tenantFencesBucket).Delete([]byte(migration.TenantDigest)); err != nil {
			return err
		}
		return tx.Bucket(tenantBundlesBucket).Delete([]byte(migration.ID))
	})
}

func (r *Repository) AbortDestination(migration ownership.Migration) error {
	if migration.Phase != ownership.PhaseAborting && migration.Phase != ownership.PhaseAborted {
		return queue.ErrTenantOwnershipConflict
	}
	return r.update(func(tx *bolt.Tx) error {
		fence, found, err := readFence(tx, migration.TenantDigest)
		if err != nil || !found {
			return err
		}
		if fence.MigrationID != migration.ID || fence.State != ownership.FencePrepared {
			return queue.ErrTenantOwnershipConflict
		}
		bundle, found, err := readBundle(tx, migration.ID)
		if err != nil {
			return err
		}
		if found {
			if err := deleteTenantBundleEntries(tx, bundle); err != nil {
				return err
			}
		}
		if err := tx.Bucket(tenantFencesBucket).Delete([]byte(migration.TenantDigest)); err != nil {
			return err
		}
		return tx.Bucket(tenantBundlesBucket).Delete([]byte(migration.ID))
	})
}

func captureTenantBundle(tx *bolt.Tx, migration ownership.Migration) (ownership.Bundle, error) {
	prefix := []byte("tn_" + migration.TenantDigest + "_")
	entries := []ownership.BundleEntry{}
	queueNames := make(map[string]struct{})
	queueIDs := make(map[string]struct{})
	messageIDs := make(map[string]struct{})
	for _, bucketName := range tenantNamedBuckets {
		bucket := tx.Bucket(bucketName)
		cursor := bucket.Cursor()
		for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			entries = append(entries, bundleEntry(bucketName, nil, key, value))
			queueNames[string(key)] = struct{}{}
			if string(bucketName) == string(queueIdentitiesBucket) {
				var identity queueIdentityRecord
				if err := decodeRecord(value, &identity); err != nil {
					return ownership.Bundle{}, err
				}
				queueIDs[identity.ID] = struct{}{}
			}
		}
	}
	moveCursor := tx.Bucket(moveTasksBucket).Cursor()
	for key, value := moveCursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = moveCursor.Next() {
		entries = append(entries, bundleEntry(moveTasksBucket, nil, key, value))
	}
	for queueName := range queueNames {
		ordered := tx.Bucket(messageOrderBucket).Bucket([]byte(queueName))
		if ordered != nil {
			if err := ordered.ForEach(func(key, value []byte) error {
				entries = append(entries, bundleEntry(messageOrderBucket, []byte(queueName), key, value))
				return nil
			}); err != nil {
				return ownership.Bundle{}, err
			}
		}
	}
	if err := tx.Bucket(messagesBucket).ForEach(func(key, value []byte) error {
		var message messageRecord
		if err := decodeRecord(value, &message); err != nil {
			return err
		}
		if _, ok := queueNames[message.QueueName]; ok {
			entries = append(entries, bundleEntry(messagesBucket, nil, key, value))
			messageIDs[string(key)] = struct{}{}
		}
		return nil
	}); err != nil {
		return ownership.Bundle{}, err
	}
	for _, bucketName := range tenantMessageBuckets[1:] {
		bucket := tx.Bucket(bucketName)
		for messageID := range messageIDs {
			if value := bucket.Get([]byte(messageID)); value != nil {
				entries = append(entries, bundleEntry(bucketName, nil, []byte(messageID), value))
			}
		}
	}
	if err := tx.Bucket(receiptsBucket).ForEach(func(key, value []byte) error {
		var receipt receiptRecord
		if err := decodeRecord(value, &receipt); err != nil {
			return err
		}
		if _, ok := queueNames[receipt.QueueName]; ok {
			entries = append(entries, bundleEntry(receiptsBucket, nil, key, value))
		}
		return nil
	}); err != nil {
		return ownership.Bundle{}, err
	}
	for _, bucketName := range [][]byte{fifoDedupBucket, fifoAttemptsBucket} {
		bucket := tx.Bucket(bucketName)
		for queueID := range queueIDs {
			idPrefix := []byte(queueID + "\x00")
			cursor := bucket.Cursor()
			for key, value := cursor.Seek(idPrefix); key != nil && bytes.HasPrefix(key, idPrefix); key, value = cursor.Next() {
				entries = append(entries, bundleEntry(bucketName, nil, key, value))
			}
		}
	}
	if value := tx.Bucket(tenantUsageBucket).Get([]byte(migration.TenantDigest)); value != nil {
		entries = append(entries, bundleEntry(tenantUsageBucket, nil, []byte(migration.TenantDigest), value))
	}
	proposalPrefix := []byte("td_" + migration.TenantDigest + "/")
	proposalCursor := tx.Bucket(replicationProposalsBucket).Cursor()
	for key, value := proposalCursor.Seek(proposalPrefix); key != nil && bytes.HasPrefix(key, proposalPrefix); key, value = proposalCursor.Next() {
		entries = append(entries, bundleEntry(replicationProposalsBucket, nil, key, value))
	}
	bundle := ownership.Bundle{Version: ownership.RecordVersion, MigrationID: migration.ID, TenantDigest: migration.TenantDigest, SourceEpoch: migration.SourceEpoch, Entries: entries}
	if err := ownership.CanonicalizeBundle(&bundle); err != nil {
		return ownership.Bundle{}, err
	}
	return bundle, nil
}

func bundleEntry(bucket, subBucket, key, value []byte) ownership.BundleEntry {
	return ownership.BundleEntry{Bucket: string(bucket), SubBucket: string(subBucket), Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)}
}

func applyTenantBundle(tx *bolt.Tx, bundle ownership.Bundle) error {
	queueNames := make(map[string]struct{})
	for _, entry := range bundle.Entries {
		bucket, err := tenantBundleBucket(tx, entry, true)
		if err != nil {
			return err
		}
		if bucket.Get(entry.Key) != nil {
			return queue.ErrTenantOwnershipConflict
		}
		value := entry.Value
		if entry.Bucket == string(replicationProposalsBucket) {
			var proposal replicationProposalRecord
			if err := decodeRecord(value, &proposal); err != nil {
				return err
			}
			proposal.FirstAppliedIndex = 1
			var err error
			value, err = encodeRecord(proposal)
			if err != nil {
				return err
			}
		}
		if err := bucket.Put(entry.Key, value); err != nil {
			return err
		}
		if entry.Bucket == string(queuesBucket) {
			queueNames[string(entry.Key)] = struct{}{}
		}
	}
	for queueName := range queueNames {
		if _, err := tx.Bucket(messageOrderBucket).CreateBucketIfNotExists([]byte(queueName)); err != nil {
			return err
		}
	}
	if err := incrementNamespaceRevision(tx); err != nil {
		return err
	}
	return incrementRedriveRevision(tx)
}

func deleteTenantBundleEntries(tx *bolt.Tx, bundle ownership.Bundle) error {
	queueNames := make(map[string]struct{})
	for _, entry := range bundle.Entries {
		bucket, err := tenantBundleBucket(tx, entry, false)
		if err != nil {
			return err
		}
		if bucket != nil {
			if current := bucket.Get(entry.Key); current != nil && entry.Bucket != string(replicationProposalsBucket) && !bytes.Equal(current, entry.Value) {
				return queue.ErrTenantOwnershipConflict
			}
			if err := bucket.Delete(entry.Key); err != nil {
				return err
			}
		}
		if entry.Bucket == string(queuesBucket) {
			queueNames[string(entry.Key)] = struct{}{}
		}
	}
	orders := tx.Bucket(messageOrderBucket)
	for queueName := range queueNames {
		if orders.Bucket([]byte(queueName)) != nil {
			if err := orders.DeleteBucket([]byte(queueName)); err != nil {
				return err
			}
		}
	}
	if err := incrementNamespaceRevision(tx); err != nil {
		return err
	}
	return incrementRedriveRevision(tx)
}

func tenantBundleBucket(tx *bolt.Tx, entry ownership.BundleEntry, create bool) (*bolt.Bucket, error) {
	allowed := false
	allowedBuckets := make([][]byte, 0, len(tenantNamedBuckets)+len(tenantMessageBuckets)+7)
	allowedBuckets = append(allowedBuckets, tenantNamedBuckets...)
	allowedBuckets = append(allowedBuckets, tenantMessageBuckets...)
	allowedBuckets = append(allowedBuckets, receiptsBucket, moveTasksBucket, fifoDedupBucket, fifoAttemptsBucket, tenantUsageBucket, replicationProposalsBucket, messageOrderBucket)
	for _, name := range allowedBuckets {
		if entry.Bucket == string(name) {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, fmt.Errorf("tenant bundle contains unsupported bucket")
	}
	parent := tx.Bucket([]byte(entry.Bucket))
	if entry.SubBucket == "" {
		return parent, nil
	}
	if entry.Bucket != string(messageOrderBucket) {
		return nil, fmt.Errorf("tenant bundle contains unsupported nested bucket")
	}
	if create {
		return parent.CreateBucketIfNotExists([]byte(entry.SubBucket))
	}
	return parent.Bucket([]byte(entry.SubBucket)), nil
}

func tenantHasData(tx *bolt.Tx, digest string) bool {
	prefix := []byte("tn_" + digest + "_")
	for _, name := range append(append([][]byte{}, tenantNamedBuckets...), moveTasksBucket) {
		key, _ := tx.Bucket(name).Cursor().Seek(prefix)
		if key != nil && bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return tx.Bucket(tenantUsageBucket).Get([]byte(digest)) != nil
}

func runningTenantMoveTask(tx *bolt.Tx, digest string) bool {
	prefix := []byte("tn_" + digest + "_")
	cursor := tx.Bucket(moveTasksBucket).Cursor()
	for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
		var task moveTaskRecord
		if decodeRecord(value, &task) == nil && task.Status == string(queue.MoveTaskRunning) {
			return true
		}
	}
	return false
}

func tenantAllowsBackgroundMutation(tx *bolt.Tx, queueName string) bool {
	digest := tenantNamespaceForQueue(queueName)
	if digest == legacyTenantUsageKey {
		return true
	}
	fence, found, err := readFence(tx, digest)
	return err == nil && (!found || fence.State == ownership.FenceActive)
}

func readFence(tx *bolt.Tx, digest string) (ownership.Fence, bool, error) {
	encoded := tx.Bucket(tenantFencesBucket).Get([]byte(digest))
	if encoded == nil {
		return ownership.Fence{}, false, nil
	}
	var result ownership.Fence
	if err := decodeRecord(encoded, &result); err != nil {
		return result, false, err
	}
	return result, true, result.Validate()
}

func readBundle(tx *bolt.Tx, id string) (ownership.Bundle, bool, error) {
	encoded := tx.Bucket(tenantBundlesBucket).Get([]byte(id))
	if encoded == nil {
		return ownership.Bundle{}, false, nil
	}
	var result ownership.Bundle
	if err := decodeRecord(encoded, &result); err != nil {
		return result, false, err
	}
	return result, true, ownership.CanonicalizeBundle(&result)
}

func putRecord(bucket *bolt.Bucket, key string, value any) error {
	encoded, err := encodeRecord(value)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(key), encoded)
}

func firstError(values ...error) error {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

var _ ownership.ControlStore = (*Repository)(nil)
var _ ownership.DataStore = (*Repository)(nil)
