package boltrepo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"simq/internal/queue"
)

const (
	replicatedCommandProtocolVersionV1 uint32 = 1
	replicatedCommandProtocolVersion   uint32 = 2
	replicationProposalRecordVersion   uint32 = 1
	maxReplicationProposals                   = 100_000
	maxReplicationResponseBytes               = 4 << 20
)

type replicationProposalRecord struct {
	Version           uint32 `json:"version"`
	ProposalID        string `json:"proposal_id"`
	FirstAppliedIndex uint64 `json:"first_applied_index"`
	Response          []byte `json:"response"`
}

type ReplicatedApplyResult struct {
	Response  []byte
	Duplicate bool
}

// ApplyReplicated commits a queue mutation, its replayable response, and the
// applied Raft index in one bbolt transaction. The clustered FSM must be the
// exclusive owner of r while invoking this method.
func (r *Repository) ApplyReplicated(index uint64, proposalID string, apply func() ([]byte, error)) (ReplicatedApplyResult, error) {
	if index == 0 || !validReplicationProposalID(proposalID) || apply == nil {
		return ReplicatedApplyResult{}, fmt.Errorf("apply replicated command: invalid index, proposal ID, or callback")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ReplicatedApplyResult{}, queue.ErrRepositoryClosed
	}
	r.transactions.Lock()
	defer r.transactions.Unlock()
	var result ReplicatedApplyResult
	err := r.updateFn(func(tx *bolt.Tx) error {
		return callTransactionSafely(tx, func(tx *bolt.Tx) error {
			metadata := tx.Bucket(metadataBucket)
			proposals := tx.Bucket(replicationProposalsBucket)
			if metadata == nil || proposals == nil {
				return corruptf("replication state is missing")
			}
			encodedApplied := metadata.Get(appliedRaftIndexKey)
			if len(encodedApplied) != 8 {
				return corruptf("applied Raft index is malformed")
			}
			applied := binary.BigEndian.Uint64(encodedApplied)
			if encoded := proposals.Get([]byte(proposalID)); encoded != nil {
				var record replicationProposalRecord
				if err := decodeRecord(encoded, &record); err != nil {
					return err
				}
				if err := validateReplicationProposalRecord(proposalID, record, applied); err != nil {
					return err
				}
				if index > applied {
					next := make([]byte, 8)
					binary.BigEndian.PutUint64(next, index)
					if err := metadata.Put(appliedRaftIndexKey, next); err != nil {
						return err
					}
				}
				result = ReplicatedApplyResult{Response: append([]byte(nil), record.Response...), Duplicate: true}
				return nil
			}
			if index <= applied {
				return corruptf("new replicated command index %d does not advance applied index %d", index, applied)
			}
			if proposals.Stats().KeyN >= maxReplicationProposals {
				return fmt.Errorf("replication proposal history limit reached")
			}
			r.ambientTx.Store(tx)
			response, applyErr := apply()
			r.ambientTx.Store(nil)
			if applyErr != nil {
				return applyErr
			}
			if response == nil || len(response) > maxReplicationResponseBytes {
				return fmt.Errorf("replicated response is empty or exceeds %d bytes", maxReplicationResponseBytes)
			}
			record := replicationProposalRecord{Version: replicationProposalRecordVersion, ProposalID: proposalID, FirstAppliedIndex: index, Response: append([]byte(nil), response...)}
			encoded, err := encodeRecord(record)
			if err != nil {
				return err
			}
			if err := proposals.Put([]byte(proposalID), encoded); err != nil {
				return err
			}
			next := make([]byte, 8)
			binary.BigEndian.PutUint64(next, index)
			if err := metadata.Put(appliedRaftIndexKey, next); err != nil {
				return err
			}
			result.Response = append([]byte(nil), response...)
			return nil
		})
	})
	if r.ambientTx.Load() != nil {
		r.ambientTx.Store(nil)
	}
	if err != nil {
		return ReplicatedApplyResult{}, wrapOperationError("apply replicated command", err)
	}
	return result, nil
}

func (r *Repository) AppliedRaftIndex() (uint64, error) {
	var index uint64
	err := r.view(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(metadataBucket).Get(appliedRaftIndexKey)
		if len(encoded) != 8 {
			return corruptf("applied Raft index is malformed")
		}
		index = binary.BigEndian.Uint64(encoded)
		return nil
	})
	return index, wrapOperationError("read applied Raft index", err)
}

func (r *Repository) WriteSnapshot(writer io.Writer) error {
	if writer == nil {
		return fmt.Errorf("write repository snapshot: writer is required")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return queue.ErrRepositoryClosed
	}
	r.transactions.Lock()
	defer r.transactions.Unlock()
	err := r.db.View(func(tx *bolt.Tx) error { _, err := tx.WriteTo(writer); return err })
	return wrapOperationError("write repository snapshot", err)
}

// RestoreSnapshot validates a complete bbolt image before atomically replacing
// the node-local FSM database. A failed validation leaves the live file intact.
func (r *Repository) RestoreSnapshot(reader io.Reader) error {
	if reader == nil {
		return fmt.Errorf("restore repository snapshot: reader is required")
	}
	directory := filepath.Dir(r.path)
	temporary, err := os.CreateTemp(directory, ".simq-restore-*.db")
	if err != nil {
		return unavailable("create snapshot restore file", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return unavailable("secure snapshot restore file", err)
	}
	_, copyErr := io.Copy(temporary, io.LimitReader(reader, 1<<40))
	writeErr := errors.Join(copyErr, temporary.Sync(), temporary.Close())
	if writeErr != nil {
		return unavailable("write snapshot restore file", writeErr)
	}
	if err := validateMigrationBackup(temporaryPath, validateTransaction); err != nil {
		return fmt.Errorf("restore repository snapshot: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return queue.ErrRepositoryClosed
	}
	r.transactions.Lock()
	defer r.transactions.Unlock()
	if err := r.db.Close(); err != nil {
		return unavailable("close repository for restore", err)
	}
	old, err := os.CreateTemp(directory, ".simq-pre-restore-*.db")
	if err != nil {
		r.reopenAfterRestoreFailure()
		return unavailable("reserve pre-restore path", err)
	}
	oldPath := old.Name()
	_ = old.Close()
	_ = os.Remove(oldPath)
	if err := os.Rename(r.path, oldPath); err != nil {
		r.reopenAfterRestoreFailure()
		return unavailable("preserve pre-restore database", err)
	}
	if err := os.Rename(temporaryPath, r.path); err != nil {
		_ = os.Rename(oldPath, r.path)
		r.reopenAfterRestoreFailure()
		return unavailable("install restored database", err)
	}
	removeTemporary = false
	newDB, err := openBolt(r.path, r.openTimeout)
	if err != nil {
		_ = os.Remove(r.path)
		_ = os.Rename(oldPath, r.path)
		r.reopenAfterRestoreFailure()
		return unavailable("open restored database", err)
	}
	r.db, r.updateFn = newDB, newDB.Update
	if err := os.Remove(oldPath); err != nil {
		return unavailable("remove pre-restore database", err)
	}
	if err := syncDirectory(directory); err != nil {
		return unavailable("sync restored database directory", err)
	}
	return nil
}

func (r *Repository) reopenAfterRestoreFailure() {
	if db, err := openBolt(r.path, r.openTimeout); err == nil {
		r.db, r.updateFn = db, db.Update
	}
}

func validateReplicationState(tx *bolt.Tx) error {
	metadata := tx.Bucket(metadataBucket)
	applied := binary.BigEndian.Uint64(metadata.Get(appliedRaftIndexKey))
	return tx.Bucket(replicationProposalsBucket).ForEach(func(key, encoded []byte) error {
		if encoded == nil {
			return corruptf("replication proposals contain a nested bucket")
		}
		var record replicationProposalRecord
		if err := decodeRecord(encoded, &record); err != nil {
			return err
		}
		return validateReplicationProposalRecord(string(key), record, applied)
	})
}

func validateReplicationProposalRecord(key string, record replicationProposalRecord, applied uint64) error {
	if record.Version != replicationProposalRecordVersion || record.ProposalID != key || !validReplicationProposalID(key) || record.FirstAppliedIndex == 0 || record.FirstAppliedIndex > applied || record.Response == nil || len(record.Response) > maxReplicationResponseBytes {
		return corruptf("replication proposal record is invalid")
	}
	return nil
}

func validReplicationProposalID(value string) bool {
	if len(value) < 1 || len(value) > 160 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
