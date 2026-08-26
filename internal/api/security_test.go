package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"simq/internal/api"
	"simq/internal/auth"
	"simq/internal/observability"
	"simq/internal/queue"
	"simq/internal/shard"
	"simq/internal/storage/boltrepo"
	"simq/internal/tenant"
)

type fakeVerifier map[string]auth.Principal

func (v fakeVerifier) Verify(_ context.Context, token string) (auth.Principal, error) {
	principal, ok := v[token]
	if !ok {
		return auth.Principal{}, errors.New("invalid")
	}
	return principal, nil
}

type memoryAuditor struct {
	mu     sync.Mutex
	events []observability.AuditEvent
}

type routingMemory struct {
	*queue.MemoryRepository
	leader bool
	url    string
}

func (r *routingMemory) Clustered() bool      { return true }
func (r *routingMemory) IsLeader() bool       { return r.leader }
func (r *routingMemory) LeaderAPIURL() string { return r.url }

func (a *memoryAuditor) Record(event observability.AuditEvent) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, event)
	return nil
}
func roles(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
func secureRequest(server http.Handler, token, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func TestSecurityModeAuthenticatesAuthorizesAndScopesTenant(t *testing.T) {
	verifier := fakeVerifier{"admin-a": {Subject: "a", Tenant: "tenant-a", Roles: roles("simq.admin")}, "admin-b": {Subject: "b", Tenant: "tenant-b", Roles: roles("simq.admin")}, "reader-a": {Subject: "r", Tenant: "tenant-a", Roles: roles("simq.reader")}, "producer-a": {Subject: "p", Tenant: "tenant-a", Roles: roles("simq.producer")}}
	repository := tenant.New(queue.NewMemoryRepository(), nil)
	server := api.NewServer(api.Config{PublicBaseURL: publicBaseURL, RequestIDGenerator: func() string { return fixedRequestID }, AuthVerifier: verifier}, queue.NewService(repository))
	if response := secureRequest(server, "", "/v1/sqs/ListQueues", `{}`); response.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status=%d", response.Code)
	}
	if response := secureRequest(server, "producer-a", "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.Code != http.StatusForbidden {
		t.Fatalf("wrong role status=%d", response.Code)
	}
	if response := secureRequest(server, "admin-a", "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.Code != http.StatusOK {
		t.Fatalf("tenant A create status=%d body=%s", response.Code, response.Body.String())
	}
	if response := secureRequest(server, "admin-b", "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`); response.Code != http.StatusOK {
		t.Fatalf("tenant B create status=%d body=%s", response.Code, response.Body.String())
	}
	response := secureRequest(server, "reader-a", "/v1/sqs/ListQueues", `{}`)
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), "/queues/orders") != 1 {
		t.Fatalf("tenant A list status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestSecurityModeRateLimitMetricsAndAuditAreTenantSafe(t *testing.T) {
	principal := auth.Principal{Issuer: "issuer-secret", Subject: "principal-secret", Tenant: "tenant-secret", Roles: roles("simq.reader")}
	metrics := observability.NewMetrics(4)
	limiter := observability.NewRateLimiter(0.0001, 1, 4)
	auditor := &memoryAuditor{}
	server := api.NewServer(api.Config{
		PublicBaseURL:      publicBaseURL,
		RequestIDGenerator: func() string { return fixedRequestID },
		AuthVerifier:       fakeVerifier{"reader": principal},
		RateLimiter:        limiter,
		Metrics:            metrics,
		MetricsToken:       "metrics-secret",
		Auditor:            auditor,
	}, queue.NewService(tenant.New(queue.NewMemoryRepository(), nil)))

	first := secureRequest(server, "reader", "/v1/sqs/ListQueues", `{}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	second := secureRequest(server, "reader", "/v1/sqs/ListQueues", `{}`)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("limited status=%d body=%s", second.Code, second.Body.String())
	}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer metrics-secret")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status=%d", response.Code)
	}
	metricsText := response.Body.String()
	if strings.Contains(metricsText, principal.Tenant) || !strings.Contains(metricsText, observability.Hash(principal.Tenant)) || !strings.Contains(metricsText, `result="2xx"`) || !strings.Contains(metricsText, `result="4xx"`) {
		t.Fatalf("unsafe or incomplete metrics: %s", metricsText)
	}
	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	if len(auditor.events) != 2 {
		t.Fatalf("audit events=%d, want 2", len(auditor.events))
	}
	for _, event := range auditor.events {
		if event.PrincipalHash != observability.Hash(principal.Issuer+"\x00"+principal.Subject) || event.TenantHash != observability.Hash(principal.Tenant) || event.Time.Before(time.Now().Add(-time.Minute)) {
			t.Fatalf("unsafe audit event: %#v", event)
		}
	}
}

func TestSecurityModeStorageQuotaReturns429AndAccountsBatchEntries(t *testing.T) {
	underlying, err := boltrepo.Open(boltrepo.Config{Path: filepath.Join(t.TempDir(), "quota.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer underlying.Close()
	underlying.SetStorageQuota(queue.StorageQuota{MaxQueues: 1, MaxMessages: 1, MaxPayloadBytes: 1024})
	verifier := fakeVerifier{
		"admin":    {Subject: "admin", Tenant: "tenant-a", Roles: roles("simq.admin")},
		"producer": {Subject: "producer", Tenant: "tenant-a", Roles: roles("simq.producer")},
	}
	server := api.NewServer(api.Config{PublicBaseURL: publicBaseURL, RequestIDGenerator: func() string { return fixedRequestID }, AuthVerifier: verifier}, queue.NewService(tenant.New(underlying, nil)))
	created := secureRequest(server, "admin", "/v1/sqs/CreateQueue", `{"QueueName":"orders"}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	limitedQueue := secureRequest(server, "admin", "/v1/sqs/CreateQueue", `{"QueueName":"second"}`)
	if limitedQueue.Code != http.StatusTooManyRequests || !strings.Contains(limitedQueue.Body.String(), `"Code":"QuotaExceeded"`) {
		t.Fatalf("queue quota status=%d body=%s", limitedQueue.Code, limitedQueue.Body.String())
	}
	batch := secureRequest(server, "producer", "/v1/sqs/SendMessageBatch", `{"QueueUrl":"http://localhost:9324/queues/orders","Entries":[{"Id":"one","MessageBody":"first"},{"Id":"two","MessageBody":"second"}]}`)
	if batch.Code != http.StatusOK || strings.Count(batch.Body.String(), `"MessageId"`) != 1 || !strings.Contains(batch.Body.String(), `"Code":"QuotaExceeded"`) {
		t.Fatalf("batch quota status=%d body=%s", batch.Code, batch.Body.String())
	}
}

func TestAuthenticatedTenantSelectsShardBeforeLeaderRouting(t *testing.T) {
	left := &routingMemory{MemoryRepository: queue.NewMemoryRepository(), leader: true, url: "https://left.example"}
	right := &routingMemory{MemoryRepository: queue.NewMemoryRepository(), leader: false, url: "https://right.example"}
	manager, err := shard.New(shard.Config{DefaultShard: "left", Repositories: map[string]queue.Repository{"left": left, "right": right}})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	base := tenant.New(manager, nil)
	verifier := fakeVerifier{}
	var leaderToken, followerToken string
	for index := 0; index < 1000 && (leaderToken == "" || followerToken == ""); index++ {
		tenantID := fmt.Sprintf("tenant-%d", index)
		service := queue.NewService(base.ForTenant(tenantID))
		_, leader, _ := service.ClusterRoute()
		token := fmt.Sprintf("token-%d", index)
		verifier[token] = auth.Principal{Subject: token, Tenant: tenantID, Roles: roles("simq.reader")}
		if leader {
			leaderToken = token
		} else {
			followerToken = token
		}
	}
	if leaderToken == "" || followerToken == "" {
		t.Fatal("could not find tenants on both shards")
	}
	server := api.NewServer(api.Config{PublicBaseURL: publicBaseURL, RequestIDGenerator: func() string { return fixedRequestID }, AuthVerifier: verifier}, queue.NewService(base))
	if response := secureRequest(server, leaderToken, "/v1/sqs/ListQueues", `{}`); response.Code != http.StatusOK {
		t.Fatalf("leader-shard status=%d body=%s", response.Code, response.Body.String())
	}
	response := secureRequest(server, followerToken, "/v1/sqs/ListQueues", `{}`)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("X-SimQ-Leader") != "https://right.example" {
		t.Fatalf("follower-shard status=%d leader=%q body=%s", response.Code, response.Header().Get("X-SimQ-Leader"), response.Body.String())
	}
}
