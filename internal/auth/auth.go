package auth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

var ErrUnauthenticated = errors.New("authentication failed")

type Principal struct {
	Issuer, Subject, Tenant string
	Roles                   map[string]struct{}
}

func (p Principal) HasRole(role string) bool { _, ok := p.Roles[role]; return ok }

type Verifier interface {
	Verify(context.Context, string) (Principal, error)
}

type Config struct {
	Issuer, Audience, TenantClaim, RolesClaim, JWKSURL string
	HTTPClient                                         *http.Client
	CacheTTL, ClockSkew                                time.Duration
	Now                                                func() time.Time
	AllowHTTP                                          bool
}

type OIDCVerifier struct {
	config    Config
	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	refreshed time.Time
}

type discoveryDocument struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}
type jwkSet struct {
	Keys []struct{ Kty, Kid, Use, Alg, N, E string } `json:"keys"`
}

func NewOIDCVerifier(ctx context.Context, config Config) (*OIDCVerifier, error) {
	config.Issuer = strings.TrimRight(config.Issuer, "/")
	if config.Issuer == "" || config.Audience == "" {
		return nil, fmt.Errorf("OIDC issuer and audience are required")
	}
	if config.TenantClaim == "" {
		config.TenantClaim = "simq_tenant"
	}
	if config.RolesClaim == "" {
		config.RolesClaim = "simq_roles"
	}
	if config.CacheTTL <= 0 {
		config.CacheTTL = 15 * time.Minute
	}
	if config.ClockSkew < 0 || config.ClockSkew > 5*time.Minute {
		return nil, fmt.Errorf("OIDC clock skew must be between zero and five minutes")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	if err := requireSecureURL(config.Issuer, config.AllowHTTP); err != nil {
		return nil, fmt.Errorf("OIDC issuer: %w", err)
	}
	verifier := &OIDCVerifier{config: config, keys: make(map[string]*rsa.PublicKey)}
	if config.JWKSURL == "" {
		documentURL := config.Issuer + "/.well-known/openid-configuration"
		var document discoveryDocument
		if err := verifier.getJSON(ctx, documentURL, &document); err != nil {
			return nil, fmt.Errorf("discover OIDC issuer: %w", err)
		}
		if document.Issuer != config.Issuer {
			return nil, fmt.Errorf("discovered OIDC issuer does not match configured issuer")
		}
		config.JWKSURL = document.JWKSURI
		verifier.config.JWKSURL = document.JWKSURI
	}
	if err := requireSecureURL(verifier.config.JWKSURL, config.AllowHTTP); err != nil {
		return nil, fmt.Errorf("OIDC JWKS URL: %w", err)
	}
	if err := verifier.refresh(ctx); err != nil {
		return nil, err
	}
	return verifier, nil
}

func (v *OIDCVerifier) Verify(ctx context.Context, token string) (Principal, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(token) > 64<<10 {
		return Principal{}, ErrUnauthenticated
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	header, err := strictObject(headerBytes)
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	var alg, kid string
	if json.Unmarshal(header["alg"], &alg) != nil || json.Unmarshal(header["kid"], &kid) != nil || alg != "RS256" || kid == "" {
		return Principal{}, ErrUnauthenticated
	}
	key := v.key(kid)
	if key == nil || v.stale() {
		if err := v.refresh(ctx); err != nil {
			return Principal{}, ErrUnauthenticated
		}
		key = v.key(kid)
	}
	if key == nil {
		return Principal{}, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], signature) != nil {
		return Principal{}, ErrUnauthenticated
	}
	claims, err := strictObject(claimsBytes)
	if err != nil {
		return Principal{}, ErrUnauthenticated
	}
	var issuer, subject, tenant string
	if json.Unmarshal(claims["iss"], &issuer) != nil || issuer != v.config.Issuer || json.Unmarshal(claims["sub"], &subject) != nil || subject == "" || json.Unmarshal(claims[v.config.TenantClaim], &tenant) != nil || !validTenant(tenant) {
		return Principal{}, ErrUnauthenticated
	}
	if !audienceContains(claims["aud"], v.config.Audience) {
		return Principal{}, ErrUnauthenticated
	}
	now := v.config.Now().UTC()
	expiration, ok := numericDate(claims["exp"])
	if !ok || !now.Before(expiration.Add(v.config.ClockSkew)) {
		return Principal{}, ErrUnauthenticated
	}
	if raw := claims["nbf"]; raw != nil {
		notBefore, ok := numericDate(raw)
		if !ok || now.Add(v.config.ClockSkew).Before(notBefore) {
			return Principal{}, ErrUnauthenticated
		}
	}
	roles, ok := stringSet(claims[v.config.RolesClaim])
	if !ok {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Issuer: v.config.Issuer, Subject: subject, Tenant: tenant, Roles: roles}, nil
}

func (v *OIDCVerifier) stale() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.config.Now().Sub(v.refreshed) >= v.config.CacheTTL
}
func (v *OIDCVerifier) key(id string) *rsa.PublicKey {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.keys[id]
}
func (v *OIDCVerifier) refresh(ctx context.Context) error {
	var set jwkSet
	if err := v.getJSON(ctx, v.config.JWKSURL, &set); err != nil {
		return fmt.Errorf("refresh OIDC keys: %w", err)
	}
	keys := make(map[string]*rsa.PublicKey)
	for _, item := range set.Keys {
		if item.Kty != "RSA" || item.Kid == "" || (item.Use != "" && item.Use != "sig") || (item.Alg != "" && item.Alg != "RS256") {
			continue
		}
		modulus, err := base64.RawURLEncoding.DecodeString(item.N)
		if err != nil || len(modulus) < 256 {
			continue
		}
		exponentBytes, err := base64.RawURLEncoding.DecodeString(item.E)
		if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
			continue
		}
		exponent := 0
		for _, value := range exponentBytes {
			exponent = exponent<<8 | int(value)
		}
		if exponent < 3 || exponent%2 == 0 {
			continue
		}
		keys[item.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: exponent}
	}
	if len(keys) == 0 {
		return fmt.Errorf("OIDC JWKS contains no accepted RS256 keys")
	}
	v.mu.Lock()
	v.keys = keys
	v.refreshed = v.config.Now()
	v.mu.Unlock()
	return nil
}
func (v *OIDCVerifier) getJSON(ctx context.Context, location string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	response, err := v.config.HTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if err := requireSecureURL(response.Request.URL.String(), v.config.AllowHTTP); err != nil {
		return fmt.Errorf("OIDC endpoint redirect is not secure: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP status %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return fmt.Errorf("JSON response contains trailing data")
	}
	return nil
}

func requireSecureURL(value string, allowHTTP bool) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("must be an absolute URL without credentials, query, or fragment")
	}
	if parsed.Scheme != "https" && !(allowHTTP && parsed.Scheme == "http") {
		return fmt.Errorf("must use HTTPS")
	}
	return nil
}
func strictObject(encoded []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("expected object")
	}
	result := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("invalid key")
		}
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf("duplicate key")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, fmt.Errorf("trailing JSON")
	}
	return result, nil
}
func audienceContains(raw json.RawMessage, want string) bool {
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single == want
	}
	var many []string
	if json.Unmarshal(raw, &many) != nil {
		return false
	}
	for _, value := range many {
		if value == want {
			return true
		}
	}
	return false
}
func numericDate(raw json.RawMessage) (time.Time, bool) {
	if raw == nil {
		return time.Time{}, false
	}
	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(&number) != nil {
		return time.Time{}, false
	}
	seconds, err := number.Int64()
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0).UTC(), true
}
func stringSet(raw json.RawMessage) (map[string]struct{}, bool) {
	result := make(map[string]struct{})
	if raw == nil {
		return result, true
	}
	var single string
	if json.Unmarshal(raw, &single) == nil {
		if single == "" {
			return nil, false
		}
		result[single] = struct{}{}
		return result, true
	}
	var values []string
	if json.Unmarshal(raw, &values) != nil {
		return nil, false
	}
	for _, value := range values {
		if value == "" {
			return nil, false
		}
		result[value] = struct{}{}
	}
	return result, true
}
func validTenant(value string) bool {
	if len(value) < 1 || len(value) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
