package cluster

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"simq/internal/queue"
)

func writeClusterTLSIdentity(t *testing.T, root string) (string, string, string) {
	t.Helper()
	now := time.Now()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "SimQ test CA"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "simq-node"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPath, certPath, keyPath := filepath.Join(root, "ca.pem"), filepath.Join(root, "node.pem"), filepath.Join(root, "node-key.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(leafKey)}), 0o600); err != nil {
		t.Fatal(err)
	}
	return caPath, certPath, keyPath
}

func TestMutualTLSRaftTransportReplicates(t *testing.T) {
	root := t.TempDir()
	caPath, certPath, keyPath := writeClusterTLSIdentity(t, root)
	address := freeAddress(t)
	node, err := Open(Config{
		NodeID:             "n1",
		BindAddress:        address,
		AdvertiseAddress:   address,
		DataDir:            filepath.Join(root, "node"),
		Bootstrap:          true,
		ApplyTimeout:       2 * time.Second,
		ReadTimeout:        2 * time.Second,
		TLSCertificateFile: certPath,
		TLSPrivateKeyFile:  keyPath,
		TLSCAFile:          caPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	waitFor(t, "TLS leader", func() bool { return node.State() == raft.Leader })
	candidate := queue.Queue{ID: "q_00000000000000000000000000000001", Name: "secure", VisibilityTimeout: 30, MessageRetentionPeriod: queue.DefaultMessageRetentionPeriod}
	if _, err := node.ForOperation("create-secure").Create(candidate); err != nil {
		t.Fatal(err)
	}
}
