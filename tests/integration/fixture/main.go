package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultIssuer = "https://issuer:8443"
	audience      = "simq-m10"
	keyID         = "m10-local"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: fixture generate|serve [options]")
	}
	var err error
	switch os.Args[1] {
	case "generate":
		err = generate(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func generate(arguments []string) error {
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	output := flags.String("output", "", "fixture output directory")
	issuer := flags.String("issuer", defaultIssuer, "exact issuer URL")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *output == "" {
		return errors.New("-output is required")
	}
	if err := os.MkdirAll(*output, 0o755); err != nil {
		return err
	}

	now := time.Now().UTC()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          serialNumber(),
		Subject:               pkix.Name{CommonName: "SimQ M10 ephemeral CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(8 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(*output, "ca.pem"), "CERTIFICATE", caDER, 0o644); err != nil {
		return err
	}
	if err := issueLeaf(*output, "issuer", []string{"issuer", "localhost"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, caCertificate, caKey, now); err != nil {
		return err
	}
	if err := issueLeaf(*output, "api", []string{"localhost", "n1", "n2", "n3"}, []net.IP{net.ParseIP("127.0.0.1")}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, caCertificate, caKey, now); err != nil {
		return err
	}
	if err := issueLeaf(*output, "raft", []string{"simq-cluster", "n1-raft", "n2-raft", "n3-raft"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, caCertificate, caKey, now); err != nil {
		return err
	}

	signingKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	jwks := struct {
		Keys []map[string]string `json:"keys"`
	}{Keys: []map[string]string{{
		"kty": "RSA", "kid": keyID, "use": "sig", "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(signingKey.PublicKey.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(signingKey.PublicKey.E)).Bytes()),
	}}}
	if err := writeJSON(filepath.Join(*output, "jwks.json"), jwks, 0o644); err != nil {
		return err
	}

	tenants := tenantsByShard()
	metadata := map[string]string{}
	for shard, tenant := range tenants {
		token, signErr := signJWT(signingKey, map[string]any{
			"iss": *issuer, "aud": audience, "sub": "m10-" + shard,
			"exp": now.Add(4 * time.Hour).Unix(), "nbf": now.Add(-time.Minute).Unix(),
			"simq_tenant": tenant, "simq_roles": []string{"simq.admin"},
		})
		if signErr != nil {
			return signErr
		}
		if err := os.WriteFile(filepath.Join(*output, "tenant-"+shard+".jwt"), []byte(token+"\n"), 0o600); err != nil {
			return err
		}
		metadata[shard] = tenant
	}
	if err := writeJSON(filepath.Join(*output, "tenants.json"), metadata, 0o600); err != nil {
		return err
	}

	manifest := manifestDocument()
	if err := writeJSON(filepath.Join(*output, "manifest.json"), manifest, 0o644); err != nil {
		return err
	}
	encryptionKey := make([]byte, 32)
	if _, err := rand.Read(encryptionKey); err != nil {
		return err
	}
	adminToken, err := randomToken()
	if err != nil {
		return err
	}
	metricsToken, err := randomToken()
	if err != nil {
		return err
	}
	environment := "SIMQ_CLUSTER_ADMIN_TOKEN=" + adminToken + "\n" +
		"SIMQ_METRICS_TOKEN=" + metricsToken + "\n" +
		"SIMQ_ENCRYPTION_ACTIVE_KEY=m10\n" +
		"SIMQ_ENCRYPTION_KEYS=m10=" + base64.StdEncoding.EncodeToString(encryptionKey) + "\n"
	if err := os.WriteFile(filepath.Join(*output, "environment.env"), []byte(environment), 0o600); err != nil {
		return err
	}
	return nil
}

func serve(arguments []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	directory := flags.String("directory", "", "fixture directory")
	listen := flags.String("listen", ":8443", "HTTPS listen address")
	issuer := flags.String("issuer", defaultIssuer, "exact issuer URL")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *directory == "" {
		return errors.New("-directory is required")
	}
	jwks, err := os.ReadFile(filepath.Join(*directory, "jwks.json"))
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/.well-known/openid-configuration", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]string{"issuer": *issuer, "jwks_uri": *issuer + "/jwks.json"})
	})
	mux.HandleFunc("/jwks.json", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(jwks)
	})
	server := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	server.TLSConfig = tlsConfig()
	log.Printf("M10 test issuer listening on %s", *listen)
	return server.ListenAndServeTLS(filepath.Join(*directory, "issuer.pem"), filepath.Join(*directory, "issuer-key.pem"))
}

func tlsConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13}
}

func issueLeaf(directory, name string, dnsNames []string, ipAddresses []net.IP, usages []x509.ExtKeyUsage, ca *x509.Certificate, caKey *rsa.PrivateKey, now time.Time) error {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	template := &x509.Certificate{
		SerialNumber: serialNumber(), Subject: pkix.Name{CommonName: name},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(8 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: usages, DNSNames: dnsNames, IPAddresses: ipAddresses,
	}
	encoded, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(directory, name+".pem"), "CERTIFICATE", encoded, 0o644); err != nil {
		return err
	}
	return writePEM(filepath.Join(directory, name+"-key.pem"), "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key), 0o644)
}

func writePEM(path, blockType string, encoded []byte, mode os.FileMode) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: encoded}), mode)
}

func writeJSON(path string, value any, mode os.FileMode) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), mode)
}

func signJWT(key *rsa.PrivateKey, claims map[string]any) (string, error) {
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": keyID, "typ": "JWT"})
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func tenantsByShard() map[string]string {
	result := make(map[string]string, 2)
	for index := 0; len(result) < 2; index++ {
		tenant := fmt.Sprintf("m10-tenant-%d", index)
		digest := sha256.Sum256([]byte(tenant))
		digestText := hex.EncodeToString(digest[:])
		selected := "s0"
		var best uint64
		for shardIndex, shard := range []string{"s0", "s1"} {
			score := sha256.Sum256([]byte(digestText + "\x00" + shard))
			value := binary.BigEndian.Uint64(score[:8])
			if shardIndex == 0 || value > best {
				selected, best = shard, value
			}
		}
		if _, exists := result[selected]; !exists {
			result[selected] = tenant
		}
	}
	return result
}

func manifestDocument() map[string]any {
	replicas := func(port int) []map[string]any {
		result := make([]map[string]any, 0, 3)
		for index := 1; index <= 3; index++ {
			result = append(result, map[string]any{
				"node_id": fmt.Sprintf("n%d", index), "raft_address": fmt.Sprintf("n%d-raft:%d", index, port),
				"api_url": fmt.Sprintf("https://localhost:%d", 19423+index), "failure_domain": fmt.Sprintf("az-%d", index), "initial_voter": true,
			})
		}
		return result
	}
	return map[string]any{
		"version": 2, "cluster_id": "00112233445566778899aabbccddeeff", "default_shard": "s0",
		"shards": []map[string]any{
			{"id": "s0", "incarnation": "11112222333344445555666677778888", "initial_state": "READY", "bootstrap_node": "n1", "replicas": replicas(7100)},
			{"id": "s1", "incarnation": "9999aaaabbbbccccddddeeeeffff0000", "initial_state": "READY", "bootstrap_node": "n1", "replicas": replicas(7200)},
		},
	}
}

func randomToken() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func serialNumber() *big.Int {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	value, err := rand.Int(rand.Reader, limit)
	if err != nil {
		panic(err)
	}
	return value
}
