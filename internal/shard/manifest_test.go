package shard

import (
	"os"
	"path/filepath"
	"testing"
)

func validManifest() Manifest {
	return Manifest{Version: 1, DefaultShard: "s0", Shards: []ShardManifest{
		{ID: "s1", BootstrapNode: "n1", Replicas: []ReplicaManifest{{NodeID: "n1", RaftAddress: "127.0.0.1:11001", APIURL: "https://n1", FailureDomain: "az-a"}, {NodeID: "n2", RaftAddress: "127.0.0.1:11002", APIURL: "https://n2", FailureDomain: "az-b"}, {NodeID: "n3", RaftAddress: "127.0.0.1:11003", APIURL: "https://n3", FailureDomain: "az-c"}}},
		{ID: "s0", BootstrapNode: "n1", Replicas: []ReplicaManifest{{NodeID: "n1", RaftAddress: "127.0.0.1:10001", APIURL: "https://n1", FailureDomain: "az-a"}, {NodeID: "n2", RaftAddress: "127.0.0.1:10002", APIURL: "https://n2", FailureDomain: "az-b"}, {NodeID: "n3", RaftAddress: "127.0.0.1:10003", APIURL: "https://n3", FailureDomain: "az-c"}}},
	}}
}

func TestManifestValidationCanonicalizesRevisionAndPlacement(t *testing.T) {
	first := validManifest()
	if err := first.Validate("n1"); err != nil {
		t.Fatal(err)
	}
	second := validManifest()
	second.Shards[0], second.Shards[1] = second.Shards[1], second.Shards[0]
	if err := second.Validate("n1"); err != nil {
		t.Fatal(err)
	}
	if len(first.Revision) != 32 || first.Revision != second.Revision {
		t.Fatalf("revisions=%q/%q", first.Revision, second.Revision)
	}
	unsafe := validManifest()
	unsafe.Shards[0].Replicas[1].FailureDomain = "az-a"
	if err := unsafe.Validate("n1"); err == nil {
		t.Fatal("manifest accepted a failure domain containing quorum")
	}
}

func TestLoadManifestRejectsEveryKindOfTrailingContent(t *testing.T) {
	valid := `{"version":1,"default_shard":"s0","shards":[` +
		`{"id":"s0","bootstrap_node":"n1","replicas":[` +
		`{"node_id":"n1","raft_address":"a0","api_url":"https://n1","failure_domain":"az-a"},` +
		`{"node_id":"n2","raft_address":"a1","api_url":"https://n2","failure_domain":"az-b"},` +
		`{"node_id":"n3","raft_address":"a2","api_url":"https://n3","failure_domain":"az-c"}]},` +
		`{"id":"s1","bootstrap_node":"n1","replicas":[` +
		`{"node_id":"n1","raft_address":"b0","api_url":"https://n1","failure_domain":"az-a"},` +
		`{"node_id":"n2","raft_address":"b1","api_url":"https://n2","failure_domain":"az-b"},` +
		`{"node_id":"n3","raft_address":"b2","api_url":"https://n3","failure_domain":"az-c"}]}]}`
	for _, suffix := range []string{" {}", " trailing-garbage"} {
		path := filepath.Join(t.TempDir(), "manifest.json")
		if err := os.WriteFile(path, []byte(valid+suffix), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadManifest(path, "n1"); err == nil {
			t.Fatalf("accepted trailing suffix %q", suffix)
		}
	}
}
