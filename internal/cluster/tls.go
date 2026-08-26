package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

func newTransport(config Config) (*raft.NetworkTransport, error) {
	advertise, err := net.ResolveTCPAddr("tcp", config.AdvertiseAddress)
	if err != nil {
		return nil, err
	}
	configured := config.TLSCertificateFile != "" || config.TLSPrivateKeyFile != "" || config.TLSCAFile != "" || config.TLSServerName != ""
	if !configured {
		return raft.NewTCPTransport(config.BindAddress, advertise, 3, 10*time.Second, io.Discard)
	}
	if config.TLSCertificateFile == "" || config.TLSPrivateKeyFile == "" || config.TLSCAFile == "" {
		return nil, fmt.Errorf("cluster TLS certificate, private key, and CA files must be configured together")
	}
	certificate, err := tls.LoadX509KeyPair(config.TLSCertificateFile, config.TLSPrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load cluster TLS identity: %w", err)
	}
	caPEM, err := os.ReadFile(config.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read cluster TLS CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("cluster TLS CA contains no certificates")
	}
	listener, err := net.Listen("tcp", config.BindAddress)
	if err != nil {
		return nil, err
	}
	serverConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}
	clientConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
		ServerName:   config.TLSServerName,
	}
	stream := &tlsStreamLayer{
		Listener:  tls.NewListener(listener, serverConfig),
		advertise: advertise,
		client:    clientConfig,
	}
	return raft.NewNetworkTransport(stream, 3, 10*time.Second, io.Discard), nil
}

type tlsStreamLayer struct {
	net.Listener
	advertise net.Addr
	client    *tls.Config
}

func (s *tlsStreamLayer) Addr() net.Addr { return s.advertise }

func (s *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	connection, err := dialer.DialContext(context.Background(), "tcp", string(address))
	if err != nil {
		return nil, err
	}
	config := s.client.Clone()
	if config.ServerName == "" {
		host, _, splitErr := net.SplitHostPort(string(address))
		if splitErr != nil {
			_ = connection.Close()
			return nil, splitErr
		}
		config.ServerName = strings.Trim(host, "[]")
	}
	tlsConnection := tls.Client(connection, config)
	if err := tlsConnection.HandshakeContext(context.Background()); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return tlsConnection, nil
}
