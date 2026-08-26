package api

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"simq/internal/queue"
	"simq/internal/shard"
	"simq/internal/storage/boltrepo"
)

func apiMigrationShard(digest string) string {
	selected := "s0"
	var best uint64
	for index, id := range []string{"s0", "s1"} {
		score := sha256.Sum256([]byte(digest + "\x00" + id))
		value := binary.BigEndian.Uint64(score[:8])
		if index == 0 || value > best {
			selected, best = id, value
		}
	}
	return selected
}

func newMigrationAPI(t *testing.T) http.Handler {
	t.Helper()
	repositories := make(map[string]queue.Repository)
	for _, id := range []string{"s0", "s1"} {
		repository, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(t.TempDir(), id+".db")})
		if err != nil {
			t.Fatal(err)
		}
		repositories[id] = repository
	}
	manager, err := shard.New(shard.Config{DefaultShard: "s0", Repositories: repositories})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return NewServer(Config{ClusterAdminToken: "cluster-secret"}, queue.NewService(manager))
}

func migrationAPIRequest(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer cluster-secret")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestTenantMigrationAdminLifecycleAndStrictInput(t *testing.T) {
	handler := newMigrationAPI(t)
	digest := strings.Repeat("1", 64)
	destination := "s0"
	if apiMigrationShard(digest) == destination {
		destination = "s1"
	}
	id := "tm_00000000000000000000000000000010"
	created := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/tenant-migrations", fmt.Sprintf(`{"MigrationId":%q,"TenantDigest":%q,"DestinationShard":%q}`, id, digest, destination))
	if created.Code != http.StatusOK || !strings.Contains(created.Body.String(), `"Phase":"FREEZING"`) {
		t.Fatalf("create code=%d body=%s", created.Code, created.Body.String())
	}
	for attempt := 0; attempt < 12; attempt++ {
		advanced := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/tenant-migrations/advance", fmt.Sprintf(`{"MigrationId":%q}`, id))
		if advanced.Code != http.StatusOK {
			t.Fatalf("advance code=%d body=%s", advanced.Code, advanced.Body.String())
		}
		if strings.Contains(advanced.Body.String(), `"Phase":"COMPLETED"`) {
			break
		}
		if attempt == 11 {
			t.Fatalf("migration did not complete: %s", advanced.Body.String())
		}
	}
	listed := migrationAPIRequest(t, handler, http.MethodGet, "/v1/cluster/tenant-migrations?Limit=10", "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), id) || strings.Contains(listed.Body.String(), "tn_") {
		t.Fatalf("list code=%d body=%s", listed.Code, listed.Body.String())
	}
	malformed := migrationAPIRequest(t, handler, http.MethodPost, "/v1/cluster/tenant-migrations/advance", fmt.Sprintf(`{"MigrationId":%q,"Unexpected":true}`, id))
	if malformed.Code != http.StatusBadRequest {
		t.Fatalf("strict input code=%d body=%s", malformed.Code, malformed.Body.String())
	}
}
