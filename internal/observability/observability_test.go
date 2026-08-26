package observability

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMetricsBoundsTenantCardinalityAndLabels(t *testing.T) {
	metrics := NewMetrics(1)
	metrics.Record("tenant-secret-a", "/v1/sqs/ListQueues", "2xx", time.Second)
	metrics.Record("tenant-secret-b", "/unexpected", "secret-result", time.Second)
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, forbidden := range []string{"tenant-secret-a", "tenant-secret-b", "unexpected", "secret-result"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("metrics exposed unbounded value %q: %s", forbidden, text)
		}
	}
	for _, required := range []string{`tenant_hash="` + Hash("tenant-secret-a") + `"`, `tenant_hash="overflow"`, `action="ListQueues"`, `action="unknown"`, `result="other"`} {
		if !strings.Contains(text, required) {
			t.Fatalf("metrics missing %q: %s", required, text)
		}
	}
}

func TestRateLimiterIsTenantScopedBoundedAndRefills(t *testing.T) {
	limiter := NewRateLimiter(2, 1, 2)
	now := time.Unix(100, 0)
	if !limiter.Allow("a", now) || limiter.Allow("a", now) {
		t.Fatal("tenant a burst was not enforced")
	}
	if !limiter.Allow("b", now) {
		t.Fatal("tenant b should have an independent bucket")
	}
	if limiter.Allow("c", now) {
		t.Fatal("new tenant should fail closed after cardinality bound")
	}
	if !limiter.Allow("a", now.Add(500*time.Millisecond)) {
		t.Fatal("tenant a token did not refill")
	}
}

func TestFileAuditorAppendsStructuredJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	auditor, err := OpenFileAuditor(path)
	if err != nil {
		t.Fatal(err)
	}
	event := AuditEvent{Time: time.Unix(100, 0).UTC(), RequestID: "request", PrincipalHash: Hash("subject"), TenantHash: Hash("tenant"), Action: "ListQueues", Resource: "/v1/sqs/ListQueues", Result: "2xx"}
	if err := auditor.Record(event); err != nil {
		t.Fatal(err)
	}
	if err := auditor.Close(); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded AuditEvent
	if err := json.Unmarshal(bytes.TrimSpace(encoded), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != event {
		t.Fatalf("decoded event = %#v, want %#v", decoded, event)
	}
	if strings.Contains(string(encoded), "subject") || strings.Contains(string(encoded), `"tenant"`) {
		t.Fatalf("audit exposed raw identity: %s", encoded)
	}
}
