package api

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"simq/internal/queue"
	"simq/internal/shard"
	"simq/internal/storage/boltrepo"
	"simq/internal/topology"
)

func newTopologyAPI(t *testing.T) http.Handler {
	t.Helper()
	repositories := make(map[string]queue.Repository)
	for _, id := range []string{"s0", "s1"} {
		repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(t.TempDir(), id+".db")})
		if err != nil {
			t.Fatal(err)
		}
		repositories[id] = repository
	}
	catalog := topology.Catalog{Version: topology.RecordVersion, ClusterID: "00112233445566778899aabbccddeeff", Generation: 1, DefaultShard: "s0", DirectoryMode: topology.DirectoryLegacy, Shards: []topology.Shard{
		{ID: "s0", Incarnation: "11112222333344445555666677778888", State: topology.ShardReady, EntryAPIURL: "https://s0.example", Initial: true},
		{ID: "s1", Incarnation: "9999aaaabbbbccccddddeeeeffff0000", State: topology.ShardCandidate, EntryAPIURL: "https://s1.example"},
	}}
	manager, err := shard.New(shard.Config{DefaultShard: "s0", Repositories: repositories, TopologyCatalog: &catalog})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return NewServer(Config{ClusterAdminToken: "cluster-secret"}, queue.NewService(manager))
}

func TestTopologyAdminBackfillAndActivationLifecycle(t *testing.T) {
	handler := newTopologyAPI(t)
	initial := migrationAPIRequest(t, handler, http.MethodGet, "/v1/cluster/topology", "")
	if initial.Code != http.StatusOK || !strings.Contains(initial.Body.String(), `"directory_mode":"LEGACY"`) {
		t.Fatalf("initial code=%d body=%s", initial.Code, initial.Body.String())
	}
	premature := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/topology/shards/activate", `{"OperationId":"to_00000000000000000000000000000010","ShardId":"s1"}`)
	if premature.Code != http.StatusConflict {
		t.Fatalf("premature activation code=%d body=%s", premature.Code, premature.Body.String())
	}
	backfillID := "to_00000000000000000000000000000011"
	started := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/topology/backfill", `{"OperationId":"`+backfillID+`"}`)
	if started.Code != http.StatusOK {
		t.Fatalf("backfill code=%d body=%s", started.Code, started.Body.String())
	}
	for attempt := 0; attempt < 5; attempt++ {
		advanced := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/topology/operations/advance", `{"OperationId":"`+backfillID+`"}`)
		if advanced.Code != http.StatusOK {
			t.Fatalf("advance backfill code=%d body=%s", advanced.Code, advanced.Body.String())
		}
		if strings.Contains(advanced.Body.String(), `"phase":"COMPLETED"`) {
			break
		}
	}
	activateID := "to_00000000000000000000000000000012"
	activated := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/topology/shards/activate", `{"OperationId":"`+activateID+`","ShardId":"s1"}`)
	if activated.Code != http.StatusOK {
		t.Fatalf("activate code=%d body=%s", activated.Code, activated.Body.String())
	}
	advanced := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/topology/operations/advance", `{"OperationId":"`+activateID+`"}`)
	if advanced.Code != http.StatusOK || !strings.Contains(advanced.Body.String(), `"phase":"COMPLETED"`) {
		t.Fatalf("advance activation code=%d body=%s", advanced.Code, advanced.Body.String())
	}
	malformed := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/topology/operations/advance", `{"OperationId":"`+activateID+`","Unexpected":true}`)
	if malformed.Code != http.StatusBadRequest {
		t.Fatalf("strict topology input code=%d body=%s", malformed.Code, malformed.Body.String())
	}
}
