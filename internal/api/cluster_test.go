package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"simq/internal/api"
	"simq/internal/queue"
)

type clusteredTestRepository struct {
	queue.Repository
	leader    bool
	leaderURL string
	seen      *struct {
		sync.Mutex
		id string
	}
}

func (r *clusteredTestRepository) Clustered() bool      { return true }
func (r *clusteredTestRepository) IsLeader() bool       { return r.leader }
func (r *clusteredTestRepository) LeaderAPIURL() string { return r.leaderURL }
func (r *clusteredTestRepository) ForOperation(id string) queue.Repository {
	r.seen.Lock()
	r.seen.id = id
	r.seen.Unlock()
	return &clusteredTestRepository{Repository: r.Repository, leader: r.leader, leaderURL: r.leaderURL, seen: r.seen}
}
func (r *clusteredTestRepository) ClusterMembers() ([]queue.ClusterMember, error) {
	return []queue.ClusterMember{{ID: "n1", Address: "127.0.0.1:9001", Suffrage: "Voter"}}, nil
}
func (r *clusteredTestRepository) AddClusterVoter(string, string) error    { return nil }
func (r *clusteredTestRepository) AddClusterNonvoter(string, string) error { return nil }
func (r *clusteredTestRepository) DemoteClusterVoter(string) error         { return nil }
func (r *clusteredTestRepository) RemoveClusterServer(string) error        { return nil }
func (r *clusteredTestRepository) TriggerClusterSnapshot() error           { return nil }
func (r *clusteredTestRepository) ShardStatuses() []queue.ShardStatus {
	return []queue.ShardStatus{{ID: "s0", CatalogRevision: "revision", Leader: r.leader, LeaderURL: r.leaderURL}}
}
func (r *clusteredTestRepository) ShardMembers(string) ([]queue.ClusterMember, error) {
	return r.ClusterMembers()
}
func (r *clusteredTestRepository) AddShardVoter(_, id, address string) error {
	return r.AddClusterVoter(id, address)
}
func (r *clusteredTestRepository) AddShardNonvoter(_, id, address string) error {
	return r.AddClusterNonvoter(id, address)
}
func (r *clusteredTestRepository) DemoteShardVoter(_, id string) error {
	return r.DemoteClusterVoter(id)
}
func (r *clusteredTestRepository) RemoveShardServer(_, id string) error {
	return r.RemoveClusterServer(id)
}
func (r *clusteredTestRepository) TriggerShardSnapshot(string) error { return nil }

func TestClusterMutationRequiresAndScopesOperationID(t *testing.T) {
	seen := &struct {
		sync.Mutex
		id string
	}{}
	repository := &clusteredTestRepository{Repository: queue.NewMemoryRepository(), leader: true, seen: seen}
	server := api.NewServer(api.Config{PublicBaseURL: publicBaseURL, RequestIDGenerator: func() string { return fixedRequestID }}, queue.NewService(repository))
	request := func(operationID string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/sqs/CreateQueue", strings.NewReader(`{"QueueName":"orders"}`))
		r.Header.Set("Content-Type", "application/json")
		if operationID != "" {
			r.Header.Set("X-SimQ-Operation-Id", operationID)
		}
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		return w
	}
	if response := request(""); response.Code != http.StatusBadRequest {
		t.Fatalf("missing operation ID status = %d", response.Code)
	}
	if response := request("client-request-1"); response.Code != http.StatusOK {
		t.Fatalf("scoped request status = %d body=%s", response.Code, response.Body.String())
	}
	seen.Lock()
	defer seen.Unlock()
	if seen.id != "client-request-1" {
		t.Fatalf("scoped operation ID = %q", seen.id)
	}
}

func TestClusterFollowerReturnsConfiguredLeaderHint(t *testing.T) {
	seen := &struct {
		sync.Mutex
		id string
	}{}
	repository := &clusteredTestRepository{Repository: queue.NewMemoryRepository(), leader: false, leaderURL: "http://node-2:9324", seen: seen}
	server := api.NewServer(api.Config{PublicBaseURL: publicBaseURL, RequestIDGenerator: func() string { return fixedRequestID }}, queue.NewService(repository))
	request := httptest.NewRequest(http.MethodPost, "/v1/sqs/ListQueues", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("X-SimQ-Leader") != "http://node-2:9324" {
		t.Fatalf("response status=%d leader=%q body=%s", response.Code, response.Header().Get("X-SimQ-Leader"), response.Body.String())
	}
}

func TestClusterAdminEndpointsRequireToken(t *testing.T) {
	seen := &struct {
		sync.Mutex
		id string
	}{}
	repository := &clusteredTestRepository{Repository: queue.NewMemoryRepository(), leader: true, seen: seen}
	server := api.NewServer(api.Config{PublicBaseURL: publicBaseURL, ClusterAdminToken: "operator-secret", RequestIDGenerator: func() string { return fixedRequestID }}, queue.NewService(repository))
	request := httptest.NewRequest(http.MethodGet, "/v1/cluster/members", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/cluster/members", nil)
	request.Header.Set("Authorization", "Bearer operator-secret")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "n1") {
		t.Fatalf("authenticated status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestShardAdminEndpointsExposeCatalogAndRequireExplicitShard(t *testing.T) {
	seen := &struct {
		sync.Mutex
		id string
	}{}
	repository := &clusteredTestRepository{Repository: queue.NewMemoryRepository(), leader: true, leaderURL: "https://node", seen: seen}
	server := api.NewServer(api.Config{PublicBaseURL: publicBaseURL, ClusterAdminToken: "operator-secret", RequestIDGenerator: func() string { return fixedRequestID }}, queue.NewService(repository))
	request := httptest.NewRequest(http.MethodGet, "/v1/cluster/shards", nil)
	request.Header.Set("Authorization", "Bearer operator-secret")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"ID":"s0"`) {
		t.Fatalf("shards status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/cluster/shards/snapshot", strings.NewReader(`{"ShardId":"s0"}`))
	request.Header.Set("Authorization", "Bearer operator-secret")
	response = httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("snapshot status=%d body=%s", response.Code, response.Body.String())
	}
}
