// Package agentbootstrap handles agent-side registration with the control plane.
// It calls AgentGateway.Register with the parameters provided at agent start,
// generates CSR, and writes the returned TLS credentials to /etc/ai-services/agent-tls/
// to establish strict mTLS for the daemon.
package agentbootstrap

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	agentpb "github.com/project-ai-services/ai-services/internal/pkg/agent/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
)

const (
	// DefaultAgentTLSDir is where mTLS cert/key will be written.
	DefaultAgentTLSDir = "/etc/ai-services/agent-tls"
)

// Config holds the parameters required to register an agent with the control plane.
type Config struct {
	ControlPlaneURL string // e.g. "lpar-0.example.com:9090"
	AgentName       string // human-readable name for this worker
	PreSharedToken  string // single-use bootstrap token issued via catalog agent issue-token
	Runtime         string // "podman" (default) or "openshift"
	Labels          map[string]string
	Capabilities    map[string]string
}

// buildTLSConfig constructs a *tls.Config for connecting to the gateway.
// If ca.crt exists in tlsDir it is used to verify the server; otherwise
// InsecureSkipVerify is set (bootstrap / first-contact scenario).
func buildTLSConfig(tlsDir string, clientCert *tls.Certificate) (*tls.Config, error) {
	cfg := &tls.Config{}
	if clientCert != nil {
		cfg.Certificates = []tls.Certificate{*clientCert}
	}
	caPath := filepath.Join(tlsDir, "ca.crt")
	if _, err := os.Stat(caPath); err == nil {
		caCertPEM, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("agentbootstrap: read ca.crt: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCertPEM) {
			return nil, fmt.Errorf("agentbootstrap: parse ca.crt failed")
		}
		cfg.RootCAs = pool
	} else if os.IsNotExist(err) {
		cfg.InsecureSkipVerify = true //nolint:gosec // intentional bootstrap fallback
	} else {
		return nil, fmt.Errorf("agentbootstrap: stat ca.crt: %w", err)
	}
	return cfg, nil
}

// hasValidTLSCredentials returns true when tls.crt and tls.key both exist in
// tlsDir, the certificate has not yet expired, and a live mTLS connection to
// the gateway succeeds. The connectivity check catches revoked or otherwise
// unusable credentials before the agent skips re-registration.
func hasValidTLSCredentials(ctx context.Context, conf Config, tlsDir string) bool {
	certPath := filepath.Join(tlsDir, "tls.crt")
	keyPath := filepath.Join(tlsDir, "tls.key")

	// 1. Load and parse the key pair.
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return false
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return false
	}

	// 2. Reject expired certificates immediately.
	if !time.Now().Before(leaf.NotAfter) {
		return false
	}

	// 3. Attempt a real mTLS dial to confirm the credentials are accepted by
	//    the gateway (catches revoked certs, server-side CA rotation, etc.).
	tlsCfg, err := buildTLSConfig(tlsDir, &cert)
	if err != nil {
		logger.WarningfCtx(ctx, "agentbootstrap: TLS config error during credential check: %v", err)
		return false
	}
	conn, err := grpc.NewClient(conf.ControlPlaneURL,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	)
	if err != nil {
		logger.WarningfCtx(ctx, "agentbootstrap: credential check dial failed: %v", err)
		return false
	}
	defer conn.Close()

	// Ping the gateway with a minimal RPC that requires a valid client cert.
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client := agentpb.NewAgentGatewayClient(conn)
	_, err = client.Register(dialCtx, &agentpb.RegisterRequest{AgentName: conf.AgentName})
	// Any response (even a gRPC application-layer error) means the mTLS
	// handshake succeeded and the credentials are functional.
	if err != nil {
		code := status.Code(err)
		if code == codes.Unauthenticated || code == codes.Unavailable {
			logger.WarningfCtx(ctx, "agentbootstrap: credential check rejected by gateway (%s): %v", code, err)
			return false
		}
		// Any other status (e.g. InvalidArgument, AlreadyExists) means TLS
		// was accepted; the application rejected the empty request, which is fine.
	}
	return true
}

// Register calls AgentGateway.Register using the configuration at confPath.
// It dynamically generates a private key, submits a CSR, and writes the resulting
// TLS material to tlsDir for mTLS operations.
// If valid mTLS credentials already exist in tlsDir, registration is skipped.
func Register(ctx context.Context, conf Config, tlsDir string) error {
	if hasValidTLSCredentials(ctx, conf, tlsDir) {
		logger.InfofCtx(ctx, "agentbootstrap: valid mTLS credentials found in %s, skipping registration", tlsDir)
		return nil
	}

	if conf.PreSharedToken == "" {
		return fmt.Errorf("agentbootstrap: no valid mTLS credentials found in %s and no pre-shared token provided", tlsDir)
	}

	// 1. Generate Local ECDSA Private Key
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("agentbootstrap: key generation failed: %w", err)
	}
	keyBytes, _ := x509.MarshalECPrivateKey(privKey)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	// 2. Generate CSR bound to the AgentID
	csrTemplate := x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   conf.AgentName,
			Organization: []string{"system:agents"},
		},
		SignatureAlgorithm: x509.ECDSAWithSHA256,
	}
	csrBytes, err := x509.CreateCertificateRequest(rand.Reader, &csrTemplate, privKey)
	if err != nil {
		return fmt.Errorf("agentbootstrap: CSR generation failed: %w", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrBytes})

	// 3. Build TLS config for the unauthenticated bootstrap connection (no client cert yet).
	tlsConfig, err := buildTLSConfig(tlsDir, nil)
	if err != nil {
		return err
	}
	if tlsConfig.InsecureSkipVerify {
		logger.WarningfCtx(ctx, "agentbootstrap: ca.crt not found at %s, falling back to blind trust (InsecureSkipVerify)", filepath.Join(tlsDir, "ca.crt"))
	} else {
		logger.InfofCtx(ctx, "agentbootstrap: ca.crt found, strict server verification enabled")
	}

	// 4. Connect to Gateway using the conditionally configured TLS
	conn, err := grpc.NewClient(conf.ControlPlaneURL,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
	)
	if err != nil {
		return fmt.Errorf("agentbootstrap: dial %s: %w", conf.ControlPlaneURL, err)
	}
	defer conn.Close()

	// 5. Submit Registration via RPC
	client := agentpb.NewAgentGatewayClient(conn)
	resp, err := client.Register(ctx, &agentpb.RegisterRequest{
		AgentName:      conf.AgentName,
		PreSharedToken: conf.PreSharedToken,
		CsrPem:         csrPEM,
		Labels:         conf.Labels,
		Capabilities:   conf.Capabilities,
	})
	if err != nil {
		return fmt.Errorf("agentbootstrap: Register RPC failed: %w", err)
	}

	logger.InfofCtx(ctx, "agentbootstrap: registered as agent_name=%s", resp.GetAgentName())

	// 6. Write the locally generated key and the officially signed certificate to disk
	if len(resp.GetTlsCertPem()) > 0 {
		if err := writeTLSMaterial(tlsDir, resp.GetTlsCertPem(), keyPEM); err != nil {
			return err
		}
		logger.InfofCtx(ctx, "agentbootstrap: TLS files are written to %s", tlsDir)
	}

	return nil
}

// writeTLSMaterial securely saves the signed certificate and local private key to disk.
func writeTLSMaterial(dir string, certPEM, keyPEM []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("agentbootstrap: mkdir %s: %w", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), certPEM, 0o644); err != nil {
		return fmt.Errorf("agentbootstrap: write cert: %w", err)
	}
	// Secure permissions (0600) for the local private key
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), keyPEM, 0o600); err != nil {
		return fmt.Errorf("agentbootstrap: write key: %w", err)
	}
	return nil
}
