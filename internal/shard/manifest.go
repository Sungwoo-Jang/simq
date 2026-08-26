package shard

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

const ManifestVersion = 1

type Manifest struct {
	Version      int             `json:"version"`
	DefaultShard string          `json:"default_shard"`
	Shards       []ShardManifest `json:"shards"`
	Revision     string          `json:"-"`
}

type ShardManifest struct {
	ID            string            `json:"id"`
	BootstrapNode string            `json:"bootstrap_node"`
	Replicas      []ReplicaManifest `json:"replicas"`
}

type ReplicaManifest struct {
	NodeID        string `json:"node_id"`
	RaftAddress   string `json:"raft_address"`
	APIURL        string `json:"api_url"`
	FailureDomain string `json:"failure_domain"`
	InitialVoter  bool   `json:"initial_voter,omitempty"`
}

func LoadManifest(path, localNodeID string) (Manifest, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("read shard manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode shard manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Manifest{}, fmt.Errorf("shard manifest contains trailing JSON")
	}
	if err := manifest.Validate(localNodeID); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (m *Manifest) Validate(localNodeID string) error {
	if m.Version != ManifestVersion || !validShardID(m.DefaultShard) || len(m.Shards) < 2 {
		return fmt.Errorf("shard manifest requires version 1, a valid default shard, and at least two shards")
	}
	sort.Slice(m.Shards, func(i, j int) bool { return m.Shards[i].ID < m.Shards[j].ID })
	seenShards := make(map[string]struct{}, len(m.Shards))
	seenAddresses := make(map[string]struct{})
	defaultFound := false
	for shardIndex := range m.Shards {
		shard := &m.Shards[shardIndex]
		if !validShardID(shard.ID) {
			return fmt.Errorf("invalid shard ID %q", shard.ID)
		}
		if _, duplicate := seenShards[shard.ID]; duplicate {
			return fmt.Errorf("duplicate shard ID %q", shard.ID)
		}
		seenShards[shard.ID] = struct{}{}
		defaultFound = defaultFound || shard.ID == m.DefaultShard
		if len(shard.Replicas) < 3 {
			return fmt.Errorf("shard %q must have at least three planned replicas", shard.ID)
		}
		sort.Slice(shard.Replicas, func(i, j int) bool { return shard.Replicas[i].NodeID < shard.Replicas[j].NodeID })
		seenNodes := make(map[string]struct{}, len(shard.Replicas))
		initialVoters := 0
		explicitInitialVoters := false
		localFound, bootstrapFound := false, false
		for _, replica := range shard.Replicas {
			if replica.NodeID == "" || replica.RaftAddress == "" || replica.APIURL == "" || replica.FailureDomain == "" {
				return fmt.Errorf("shard %q replica fields must be non-empty", shard.ID)
			}
			if _, duplicate := seenNodes[replica.NodeID]; duplicate {
				return fmt.Errorf("shard %q has duplicate node %q", shard.ID, replica.NodeID)
			}
			seenNodes[replica.NodeID] = struct{}{}
			if _, duplicate := seenAddresses[replica.RaftAddress]; duplicate {
				return fmt.Errorf("Raft address %q is reused across shard replicas", replica.RaftAddress)
			}
			seenAddresses[replica.RaftAddress] = struct{}{}
			if replica.InitialVoter {
				explicitInitialVoters = true
				initialVoters++
			}
			localFound = localFound || replica.NodeID == localNodeID
			bootstrapFound = bootstrapFound || replica.NodeID == shard.BootstrapNode
		}
		if !localFound {
			return fmt.Errorf("local node %q is not a replica of shard %q", localNodeID, shard.ID)
		}
		if shard.BootstrapNode == "" || !bootstrapFound {
			return fmt.Errorf("shard %q bootstrap node is not a replica", shard.ID)
		}
		if !explicitInitialVoters {
			initialVoters = len(shard.Replicas)
		}
		if explicitInitialVoters {
			bootstrapFound = false
			for _, replica := range shard.Replicas {
				bootstrapFound = bootstrapFound || replica.NodeID == shard.BootstrapNode && replica.InitialVoter
			}
			if !bootstrapFound {
				return fmt.Errorf("shard %q bootstrap node must be an initial voter", shard.ID)
			}
		}
		if initialVoters < 3 || initialVoters%2 == 0 {
			return fmt.Errorf("shard %q must have an odd initial voter count of at least three", shard.ID)
		}
		quorum := initialVoters/2 + 1
		initialDomains := make(map[string]int)
		for _, replica := range shard.Replicas {
			if !explicitInitialVoters || replica.InitialVoter {
				initialDomains[replica.FailureDomain]++
			}
		}
		for domain, count := range initialDomains {
			if count >= quorum {
				return fmt.Errorf("shard %q places quorum %d in failure domain %q", shard.ID, count, domain)
			}
		}
	}
	if !defaultFound {
		return fmt.Errorf("default shard %q is absent", m.DefaultShard)
	}
	shardIDs := make([]string, len(m.Shards))
	for index, shard := range m.Shards {
		shardIDs[index] = shard.ID
	}
	canonical, err := json.Marshal(struct {
		Version      int      `json:"version"`
		DefaultShard string   `json:"default_shard"`
		ShardIDs     []string `json:"shard_ids"`
	}{m.Version, m.DefaultShard, shardIDs})
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	m.Revision = hex.EncodeToString(digest[:16])
	return nil
}

func (m Manifest) LocalReplica(shard ShardManifest, nodeID string) (ReplicaManifest, bool) {
	for _, replica := range shard.Replicas {
		if replica.NodeID == nodeID {
			return replica, true
		}
	}
	return ReplicaManifest{}, false
}
