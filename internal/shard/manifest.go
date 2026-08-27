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

	"simq/internal/topology"
)

const (
	ManifestVersion   = 1
	ManifestVersionV2 = 2
)

type Manifest struct {
	Version      int             `json:"version"`
	ClusterID    string          `json:"cluster_id,omitempty"`
	DefaultShard string          `json:"default_shard"`
	Shards       []ShardManifest `json:"shards"`
	Revision     string          `json:"-"`
}

type ShardManifest struct {
	ID            string              `json:"id"`
	Incarnation   string              `json:"incarnation,omitempty"`
	InitialState  topology.ShardState `json:"initial_state,omitempty"`
	BootstrapNode string              `json:"bootstrap_node"`
	Replicas      []ReplicaManifest   `json:"replicas"`
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
	if (m.Version != ManifestVersion && m.Version != ManifestVersionV2) || !validShardID(m.DefaultShard) || len(m.Shards) < 2 {
		return fmt.Errorf("shard manifest requires version 1 or 2, a valid default shard, and at least two shards")
	}
	if m.Version == ManifestVersionV2 && !topology.ValidIdentity(m.ClusterID) {
		return fmt.Errorf("version 2 shard manifest requires a 32-character lowercase hexadecimal cluster_id")
	}
	if m.Version == ManifestVersion && m.ClusterID != "" {
		return fmt.Errorf("version 1 shard manifest does not accept cluster_id")
	}
	sort.Slice(m.Shards, func(i, j int) bool { return m.Shards[i].ID < m.Shards[j].ID })
	seenShards := make(map[string]struct{}, len(m.Shards))
	seenIncarnations := make(map[string]struct{}, len(m.Shards))
	seenAddresses := make(map[string]struct{})
	defaultFound := false
	for shardIndex := range m.Shards {
		shard := &m.Shards[shardIndex]
		if !validShardID(shard.ID) {
			return fmt.Errorf("invalid shard ID %q", shard.ID)
		}
		if m.Version == ManifestVersionV2 {
			if !topology.ValidIdentity(shard.Incarnation) || shard.InitialState != topology.ShardReady && shard.InitialState != topology.ShardCandidate {
				return fmt.Errorf("shard %q requires an incarnation and READY or CANDIDATE initial_state", shard.ID)
			}
			if _, duplicate := seenIncarnations[shard.Incarnation]; duplicate {
				return fmt.Errorf("duplicate shard incarnation %q", shard.Incarnation)
			}
			seenIncarnations[shard.Incarnation] = struct{}{}
			if shard.ID == m.DefaultShard && shard.InitialState != topology.ShardReady {
				return fmt.Errorf("default shard %q must initially be READY", shard.ID)
			}
		} else if shard.Incarnation != "" || shard.InitialState != "" {
			return fmt.Errorf("version 1 shard %q does not accept M9 topology fields", shard.ID)
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
		if !localFound && (m.Version == ManifestVersion || shard.ID == m.DefaultShard) {
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
	if m.Version == ManifestVersionV2 {
		var defaultShard ShardManifest
		for _, shard := range m.Shards {
			if shard.ID == m.DefaultShard {
				defaultShard = shard
				break
			}
		}
		explicitDefaultVoters := false
		for _, replica := range defaultShard.Replicas {
			explicitDefaultVoters = explicitDefaultVoters || replica.InitialVoter
		}
		for _, replica := range defaultShard.Replicas {
			if explicitDefaultVoters && !replica.InitialVoter {
				continue
			}
			for _, shard := range m.Shards {
				found := false
				for _, candidate := range shard.Replicas {
					found = found || candidate.NodeID == replica.NodeID
				}
				if !found {
					return fmt.Errorf("default voter %q must be a planned replica of shard %q", replica.NodeID, shard.ID)
				}
			}
		}
		m.Revision = m.ClusterID
		return nil
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
	for index := range m.Shards {
		incarnation := sha256.Sum256([]byte(m.Revision + "\x00" + m.Shards[index].ID))
		m.Shards[index].Incarnation = hex.EncodeToString(incarnation[:16])
		m.Shards[index].InitialState = topology.ShardReady
	}
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
