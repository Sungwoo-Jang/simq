package topology

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"simq/internal/ownership"
)

const RecordVersion uint32 = 1

type DirectoryMode string

const (
	DirectoryLegacy      DirectoryMode = "LEGACY"
	DirectoryBackfilling DirectoryMode = "BACKFILLING"
	DirectoryExplicit    DirectoryMode = "EXPLICIT"
)

type ShardState string

const (
	ShardCandidate ShardState = "CANDIDATE"
	ShardReady     ShardState = "READY"
	ShardDraining  ShardState = "DRAINING"
	ShardRetired   ShardState = "RETIRED"
)

type OperationKind string

const (
	OperationBackfill OperationKind = "BACKFILL"
	OperationActivate OperationKind = "ACTIVATE"
	OperationDrain    OperationKind = "DRAIN"
)

type OperationPhase string

const (
	PhasePlanned   OperationPhase = "PLANNED"
	PhaseRunning   OperationPhase = "RUNNING"
	PhaseAborting  OperationPhase = "ABORTING"
	PhaseCompleted OperationPhase = "COMPLETED"
	PhaseAborted   OperationPhase = "ABORTED"
)

type Shard struct {
	ID          string     `json:"id"`
	Incarnation string     `json:"incarnation"`
	State       ShardState `json:"state"`
	EntryAPIURL string     `json:"entry_api_url"`
	Initial     bool       `json:"initial"`
}

type Catalog struct {
	Version          uint32        `json:"version"`
	ClusterID        string        `json:"cluster_id"`
	Generation       uint64        `json:"generation"`
	DefaultShard     string        `json:"default_shard"`
	DirectoryMode    DirectoryMode `json:"directory_mode"`
	BackfillComplete bool          `json:"backfill_complete"`
	Shards           []Shard       `json:"shards"`
}

type Operation struct {
	Version           uint32         `json:"version"`
	ID                string         `json:"id"`
	Kind              OperationKind  `json:"kind"`
	ShardID           string         `json:"shard_id"`
	Phase             OperationPhase `json:"phase"`
	Generation        uint64         `json:"generation"`
	ShardIndex        int            `json:"shard_index"`
	AfterDigest       string         `json:"after_digest"`
	ActiveMigrationID string         `json:"active_migration_id"`
	MovedCount        uint64         `json:"moved_count"`
}

type Tombstone struct {
	Version     uint32 `json:"version"`
	Generation  uint64 `json:"generation"`
	ShardID     string `json:"shard_id"`
	Incarnation string `json:"incarnation"`
}

type BeginOperationCommand struct {
	OperationID string
	Kind        OperationKind
	ShardID     string
}

type BackfillBatchCommand struct {
	OperationID     string
	ShardIndex      int
	AfterDigest     string
	Records         []ownership.Record
	NextShardIndex  int
	NextAfterDigest string
}

type DrainProgressCommand struct {
	OperationID, ExpectedMigrationID, NextMigrationID string
	IncrementMoved                                    bool
}

type Status struct {
	Operation     Operation `json:"Operation"`
	NextAction    string    `json:"NextAction,omitempty"`
	NextShard     string    `json:"NextShard,omitempty"`
	NextLeaderURL string    `json:"NextLeaderUrl,omitempty"`
	LocalLeader   bool      `json:"LocalLeader"`
}

type ControlStore interface {
	Catalog() (Catalog, bool, error)
	LocalCatalog() (Catalog, bool, error)
	InitializeCatalog(Catalog) (Catalog, error)
	EnsureAssignment(string) (ownership.Record, error)
	OwnersByShard(string, string, int) ([]ownership.Record, bool, error)
	Operation(string) (Operation, bool, error)
	LocalOperation(string) (Operation, bool, error)
	ListOperations(int) ([]Operation, error)
	BeginTopologyOperation(BeginOperationCommand) (Operation, error)
	ApplyBackfillBatch(BackfillBatchCommand) (Operation, error)
	CompleteBackfill(string) (Operation, error)
	ActivateShard(string) (Operation, error)
	UpdateDrainProgress(DrainProgressCommand) (Operation, error)
	RetireShard(string) (Operation, error)
	AbortTopologyOperation(string) (Operation, error)
}

type DataStore interface {
	LocalTenantDigests(string, int) ([]string, bool, error)
}

func ValidShardID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func ValidIdentity(value string) bool { return validHex(value, 16) }
func ValidOperationID(value string) bool {
	return len(value) == 35 && strings.HasPrefix(value, "to_") && validHex(value[3:], 16)
}
func validHex(value string, bytes int) bool {
	if value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func validEntryURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" && parsed.User == nil && parsed.Fragment == ""
}

func (s Shard) Validate() error {
	if !ValidShardID(s.ID) || !ValidIdentity(s.Incarnation) || !validEntryURL(s.EntryAPIURL) {
		return fmt.Errorf("invalid topology shard")
	}
	switch s.State {
	case ShardCandidate, ShardReady, ShardDraining, ShardRetired:
		return nil
	default:
		return fmt.Errorf("invalid topology shard state")
	}
}

func (c *Catalog) Canonicalize() error {
	if c.Version != RecordVersion || !ValidIdentity(c.ClusterID) || c.Generation == 0 || !ValidShardID(c.DefaultShard) || len(c.Shards) == 0 {
		return fmt.Errorf("invalid topology catalog")
	}
	switch c.DirectoryMode {
	case DirectoryLegacy, DirectoryBackfilling, DirectoryExplicit:
	default:
		return fmt.Errorf("invalid tenant directory mode")
	}
	if c.BackfillComplete != (c.DirectoryMode == DirectoryExplicit) {
		return fmt.Errorf("tenant directory completion state conflicts with its mode")
	}
	sort.Slice(c.Shards, func(i, j int) bool { return c.Shards[i].ID < c.Shards[j].ID })
	defaultFound := false
	for index := range c.Shards {
		if err := c.Shards[index].Validate(); err != nil {
			return err
		}
		if index > 0 && c.Shards[index-1].ID == c.Shards[index].ID {
			return fmt.Errorf("duplicate topology shard")
		}
		if c.Shards[index].ID == c.DefaultShard {
			defaultFound = c.Shards[index].State == ShardReady && c.Shards[index].Initial
		}
	}
	if !defaultFound {
		return fmt.Errorf("default shard must be an initial READY shard")
	}
	return nil
}

func (o Operation) Validate() error {
	if o.Version != RecordVersion || !ValidOperationID(o.ID) || o.Generation == 0 || o.ShardIndex < 0 || o.AfterDigest != "" && !ownership.ValidDigest(o.AfterDigest) || o.ActiveMigrationID != "" && !ownership.ValidMigrationID(o.ActiveMigrationID) {
		return fmt.Errorf("invalid topology operation")
	}
	switch o.Kind {
	case OperationBackfill:
		if o.ShardID != "" {
			return fmt.Errorf("backfill operation cannot name a shard")
		}
	case OperationActivate, OperationDrain:
		if !ValidShardID(o.ShardID) {
			return fmt.Errorf("topology operation shard is invalid")
		}
	default:
		return fmt.Errorf("invalid topology operation kind")
	}
	switch o.Phase {
	case PhasePlanned, PhaseRunning, PhaseAborting, PhaseCompleted, PhaseAborted:
		return nil
	default:
		return fmt.Errorf("invalid topology operation phase")
	}
}

func SelectShard(digest string, catalog Catalog, legacy bool, exclude string) (string, error) {
	if !ownership.ValidDigest(digest) {
		return "", fmt.Errorf("invalid tenant digest")
	}
	if err := catalog.Canonicalize(); err != nil {
		return "", err
	}
	selected := ""
	var best uint64
	for _, shard := range catalog.Shards {
		if shard.ID == exclude || shard.State != ShardReady || legacy && !shard.Initial {
			continue
		}
		score := sha256.Sum256([]byte(digest + "\x00" + shard.ID))
		value := binary.BigEndian.Uint64(score[:8])
		if selected == "" || value > best {
			selected, best = shard.ID, value
		}
	}
	if selected == "" {
		return "", fmt.Errorf("no READY shard is available")
	}
	return selected, nil
}

func FindShard(c Catalog, id string) (Shard, bool) {
	for _, shard := range c.Shards {
		if shard.ID == id {
			return shard, true
		}
	}
	return Shard{}, false
}
