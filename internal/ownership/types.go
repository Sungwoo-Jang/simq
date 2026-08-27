package ownership

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	RecordVersion  uint32 = 1
	MaxBundleBytes        = 16 << 20
)

type Phase string

const (
	PhaseFreezing    Phase = "FREEZING"
	PhasePreparing   Phase = "PREPARING"
	PhaseCuttingOver Phase = "CUTTING_OVER"
	PhaseActivating  Phase = "ACTIVATING"
	PhaseCleaning    Phase = "CLEANING"
	PhaseAborting    Phase = "ABORTING"
	PhaseCompleted   Phase = "COMPLETED"
	PhaseAborted     Phase = "ABORTED"
)

type FenceState string

const (
	FenceFrozen   FenceState = "FROZEN"
	FencePrepared FenceState = "PREPARED"
	FenceActive   FenceState = "ACTIVE"
	FenceMoved    FenceState = "MOVED"
)

type Record struct {
	Version      uint32 `json:"version"`
	TenantDigest string `json:"tenant_digest"`
	ShardID      string `json:"shard_id"`
	Epoch        uint64 `json:"epoch"`
}

type Migration struct {
	Version          uint32 `json:"version"`
	ID               string `json:"id"`
	TenantDigest     string `json:"tenant_digest"`
	SourceShard      string `json:"source_shard"`
	DestinationShard string `json:"destination_shard"`
	SourceEpoch      uint64 `json:"source_epoch"`
	DestinationEpoch uint64 `json:"destination_epoch"`
	Phase            Phase  `json:"phase"`
	BundleHash       string `json:"bundle_hash"`
}

type Fence struct {
	Version          uint32     `json:"version"`
	MigrationID      string     `json:"migration_id"`
	TenantDigest     string     `json:"tenant_digest"`
	Epoch            uint64     `json:"epoch"`
	State            FenceState `json:"state"`
	DestinationShard string     `json:"destination_shard"`
	BundleHash       string     `json:"bundle_hash"`
}

type BundleEntry struct {
	Bucket    string `json:"bucket"`
	SubBucket string `json:"sub_bucket,omitempty"`
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
}

type Bundle struct {
	Version      uint32        `json:"version"`
	MigrationID  string        `json:"migration_id"`
	TenantDigest string        `json:"tenant_digest"`
	SourceEpoch  uint64        `json:"source_epoch"`
	Entries      []BundleEntry `json:"entries"`
	Hash         string        `json:"hash"`
}

type BeginCommand struct {
	MigrationID, TenantDigest, SourceShard, DestinationShard string
	SourceEpoch                                              uint64
}

type TransitionCommand struct {
	MigrationID   string
	ExpectedPhase Phase
	NextPhase     Phase
	BundleHash    string
}

type Status struct {
	Migration     Migration `json:"Migration"`
	NextAction    string    `json:"NextAction,omitempty"`
	NextShard     string    `json:"NextShard,omitempty"`
	NextLeaderURL string    `json:"NextLeaderUrl,omitempty"`
	LocalLeader   bool      `json:"LocalLeader"`
}

type ControlStore interface {
	Ownership(string) (Record, bool, error)
	LocalOwnership(string) (Record, bool, error)
	Migration(string) (Migration, bool, error)
	LocalMigration(string) (Migration, bool, error)
	ListMigrations(int) ([]Migration, error)
	BeginMigration(BeginCommand) (Migration, error)
	TransitionMigration(TransitionCommand) (Migration, error)
}

type DataStore interface {
	LocalFence(string) (Fence, bool, error)
	LocalBundle(string) (Bundle, bool, error)
	FreezeAndCapture(Migration) (Fence, error)
	Prepare(Migration, Bundle) (Fence, error)
	Activate(Migration) (Fence, error)
	Cleanup(Migration) (Fence, error)
	AbortSource(Migration) error
	AbortDestination(Migration) error
}

func ValidDigest(value string) bool { return validHex(value, 32) }
func ValidHash(value string) bool   { return validHex(value, 32) }
func ValidMigrationID(value string) bool {
	return len(value) == 35 && strings.HasPrefix(value, "tm_") && validHex(value[3:], 16)
}
func validHex(value string, bytes int) bool {
	if value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func (r Record) Validate() error {
	if r.Version != RecordVersion || !ValidDigest(r.TenantDigest) || r.ShardID == "" || r.Epoch == 0 {
		return fmt.Errorf("invalid tenant ownership record")
	}
	return nil
}

func (m Migration) Validate() error {
	if m.Version != RecordVersion || !ValidMigrationID(m.ID) || !ValidDigest(m.TenantDigest) || m.SourceShard == "" || m.DestinationShard == "" || m.SourceShard == m.DestinationShard || m.SourceEpoch == 0 || m.DestinationEpoch != m.SourceEpoch+1 || !ValidPhase(m.Phase) || m.BundleHash != "" && !ValidHash(m.BundleHash) {
		return fmt.Errorf("invalid tenant migration record")
	}
	return nil
}

func ValidPhase(value Phase) bool {
	switch value {
	case PhaseFreezing, PhasePreparing, PhaseCuttingOver, PhaseActivating, PhaseCleaning, PhaseAborting, PhaseCompleted, PhaseAborted:
		return true
	default:
		return false
	}
}

func (f Fence) Validate() error {
	if f.Version != RecordVersion || !ValidMigrationID(f.MigrationID) || !ValidDigest(f.TenantDigest) || f.Epoch == 0 || f.BundleHash != "" && !ValidHash(f.BundleHash) {
		return fmt.Errorf("invalid tenant fence record")
	}
	switch f.State {
	case FenceFrozen:
		if f.DestinationShard == "" || f.BundleHash == "" {
			return fmt.Errorf("invalid frozen tenant fence")
		}
	case FencePrepared:
		if f.BundleHash == "" {
			return fmt.Errorf("invalid prepared tenant fence")
		}
	case FenceActive:
	case FenceMoved:
		if f.DestinationShard == "" {
			return fmt.Errorf("invalid moved tenant fence")
		}
	default:
		return fmt.Errorf("invalid tenant fence state")
	}
	return nil
}

func CanonicalizeBundle(bundle *Bundle) error {
	if bundle.Version != RecordVersion || !ValidMigrationID(bundle.MigrationID) || !ValidDigest(bundle.TenantDigest) || bundle.SourceEpoch == 0 {
		return fmt.Errorf("invalid tenant bundle envelope")
	}
	sort.Slice(bundle.Entries, func(i, j int) bool {
		left, right := bundle.Entries[i], bundle.Entries[j]
		if left.Bucket != right.Bucket {
			return left.Bucket < right.Bucket
		}
		if left.SubBucket != right.SubBucket {
			return left.SubBucket < right.SubBucket
		}
		return string(left.Key) < string(right.Key)
	})
	for index, entry := range bundle.Entries {
		if entry.Bucket == "" || len(entry.Key) == 0 || entry.Value == nil {
			return fmt.Errorf("invalid tenant bundle entry")
		}
		if index > 0 {
			previous := bundle.Entries[index-1]
			if previous.Bucket == entry.Bucket && previous.SubBucket == entry.SubBucket && string(previous.Key) == string(entry.Key) {
				return fmt.Errorf("duplicate tenant bundle entry")
			}
		}
	}
	hash, encodedSize, err := bundleDigest(*bundle)
	if err != nil {
		return err
	}
	if encodedSize > MaxBundleBytes {
		return fmt.Errorf("tenant bundle exceeds %d bytes", MaxBundleBytes)
	}
	if bundle.Hash != "" && bundle.Hash != hash {
		return fmt.Errorf("tenant bundle hash mismatch")
	}
	bundle.Hash = hash
	return nil
}

func bundleDigest(bundle Bundle) (string, int, error) {
	bundle.Hash = ""
	encoded, err := json.Marshal(bundle)
	if err != nil {
		return "", 0, err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), len(encoded), nil
}
