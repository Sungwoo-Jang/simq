package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

type keyServer struct {
	mu     sync.RWMutex
	keys   map[string]*rsa.PrivateKey
	issuer string
}

func (s *keyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/.well-known/openid-configuration" {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": s.issuer, "jwks_uri": s.issuer + "/keys"})
		return
	}
	items := make([]map[string]string, 0, len(s.keys))
	for id, key := range s.keys {
		items = append(items, map[string]string{"kty": "RSA", "kid": id, "use": "sig", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": items})
}
func signToken(t *testing.T, key *rsa.PrivateKey, kid, alg string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": alg, "kid": kid, "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func signRawToken(t *testing.T, key *rsa.PrivateKey, kid, payload string) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": kid, "typ": "JWT"})
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString([]byte(payload))
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func TestOIDCVerifierValidatesClaimsAlgorithmAndKeyRotation(t *testing.T) {
	key1, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	key2, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keys := &keyServer{keys: map[string]*rsa.PrivateKey{"key-1": key1}}
	server := httptest.NewServer(keys)
	defer server.Close()
	keys.issuer = server.URL
	now := time.Unix(2_000_000_000, 0).UTC()
	verifier, err := NewOIDCVerifier(context.Background(), Config{Issuer: server.URL, Audience: "simq-api", AllowHTTP: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]any{"iss": server.URL, "aud": []string{"simq-api"}, "sub": "user-1", "exp": now.Add(time.Hour).Unix(), "nbf": now.Add(-time.Minute).Unix(), "simq_tenant": "tenant-a", "simq_roles": []string{"simq.reader", "simq.producer"}}
	principal, err := verifier.Verify(context.Background(), signToken(t, key1, "key-1", "RS256", claims))
	if err != nil || principal.Tenant != "tenant-a" || !principal.HasRole("simq.producer") {
		t.Fatalf("principal=%+v err=%v", principal, err)
	}
	badAudience := mapsClone(claims)
	badAudience["aud"] = "other"
	if _, err := verifier.Verify(context.Background(), signToken(t, key1, "key-1", "RS256", badAudience)); err == nil {
		t.Fatal("bad audience accepted")
	}
	expired := mapsClone(claims)
	expired["exp"] = now.Add(-time.Second).Unix()
	if _, err := verifier.Verify(context.Background(), signToken(t, key1, "key-1", "RS256", expired)); err == nil {
		t.Fatal("expired token accepted")
	}
	if _, err := verifier.Verify(context.Background(), signToken(t, key1, "key-1", "HS256", claims)); err == nil {
		t.Fatal("unapproved algorithm accepted")
	}
	duplicateClaims := `{"iss":` + strconv.Quote(server.URL) + `,"aud":"simq-api","aud":"other","sub":"user-1","exp":` + strconv.FormatInt(now.Add(time.Hour).Unix(), 10) + `,"simq_tenant":"tenant-a","simq_roles":["simq.reader"]}`
	if _, err := verifier.Verify(context.Background(), signRawToken(t, key1, "key-1", duplicateClaims)); err == nil {
		t.Fatal("duplicate JWT claim accepted")
	}
	keys.mu.Lock()
	keys.keys["key-2"] = key2
	keys.mu.Unlock()
	if _, err := verifier.Verify(context.Background(), signToken(t, key2, "key-2", "RS256", claims)); err != nil {
		t.Fatalf("rotated key: %v", err)
	}
}

func TestOIDCDiscoveryRejectsHTTPSDowngradeRedirect(t *testing.T) {
	var issuer string
	insecureTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer, "jwks_uri": issuer + "/keys"})
	}))
	defer insecureTarget.Close()
	secureIssuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, insecureTarget.URL, http.StatusFound)
	}))
	defer secureIssuer.Close()
	issuer = secureIssuer.URL
	if _, err := NewOIDCVerifier(context.Background(), Config{Issuer: issuer, Audience: "simq", HTTPClient: secureIssuer.Client()}); err == nil {
		t.Fatal("HTTPS-to-HTTP OIDC discovery redirect accepted")
	}
}

func mapsClone(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
