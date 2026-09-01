// Package join implements the worker join workflow.
//
// The join flow:
//
//  1. Check if valid mTLS credentials already exist on disk — if so, skip
//     registration and go straight to the CommandStream using those creds.
//
//  2. Setup — Deploy the Caddy reverse-proxy pod on the worker node.
//
//  3. Bootstrap — Generate an ECDSA P-256 key + CSR locally, dial the gateway
//     with TLS (server-only), call Register with the pre-shared token and CSR.
//     The gateway returns a CA-signed client cert + the CA cert. Both are written
//     to tlsDir so all future connections use strict mTLS.
//
//  4. Connect — Open the long-lived CommandStream with mTLS and maintain it,
//     forwarding heartbeats. Retried with exponential back-off on transient
//     failures.
package join

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
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
	"github.com/project-ai-services/ai-services/internal/pkg/utils"
	workerconstants "github.com/project-ai-services/ai-services/internal/pkg/worker/constants"
	workerdeploy "github.com/project-ai-services/ai-services/internal/pkg/worker/deploy"
	"github.com/project-ai-services/ai-services/internal/pkg/worker/dispatch"
	workerpb "github.com/project-ai-services/ai-services/internal/pkg/worker/proto"
)

const (
	// DefaultWorkerTLSDir is where the worker's mTLS credentials are stored.
	DefaultWorkerTLSDir = "/etc/ai-services/worker-tls"

	// heartbeatInterval is how often the worker sends a keep-alive to the control plane.
	heartbeatInterval = 30 * time.Second

	// retryBase is the initial back-off duration before retrying CommandStream.
	retryBase = 5 * time.Second
	// retryMax caps the back-off so the worker does not wait too long.
	retryMax = 2 * time.Minute

	// retryBackoffFactor is the exponential multiplier applied to the backoff duration.
	retryBackoffFactor = 2
)

// Options carries everything needed to join a worker to the catalog control plane.
type Options struct {
	// GatewayAddr is the host:port of the catalog gRPC worker-gateway,
	// e.g. "catalog.example.com:9090".
	GatewayAddr string

	// Token is the single-use bootstrap token issued by
	// `ai-services catalog worker register`.
	Token string

	// RuntimeType is the execution environment of this worker node.
	RuntimeType types.RuntimeType

	// Setup holds the options for setting up this worker node (Caddy proxy, etc.).
	Setup workerdeploy.Options

	// TLSDir is where mTLS credentials are stored/read. Defaults to DefaultWorkerTLSDir.
	TLSDir string
}

// Run executes the complete worker join workflow and blocks until ctx is
// cancelled or an unrecoverable error occurs.
func Run(ctx context.Context, opts Options) error {
	if opts.TLSDir == "" {
		opts.TLSDir = DefaultWorkerTLSDir
	}

	domainSuffix, err := utils.ComputeDomainSuffix(opts.Setup.SSLCertPath, opts.Setup.SSLKeyPath, opts.Setup.DomainName)
	if err != nil {
		return err
	}

	rt, err := runtime.CreateRuntime(opts.RuntimeType, "")
	if err != nil {
		return fmt.Errorf("worker join: init runtime: %w", err)
	}

	meta := map[string]string{
		workerconstants.MetaKeyBaseDir:      opts.Setup.BaseDir,
		workerconstants.MetaKeyDomainSuffix: domainSuffix,
		workerconstants.MetaKeyHTTPSPort:    strconv.Itoa(opts.Setup.HTTPSPort),
	}

	// ── Step 1: Check for existing valid mTLS credentials ────────────────────
	if hasValidTLSCredentials(ctx, opts.TLSDir) {
		logger.InfofCtx(ctx, "worker join: valid mTLS credentials found in %s, skipping registration", opts.TLSDir)
		return connectAndStream(ctx, rt, opts.GatewayAddr, opts.TLSDir, meta)
	}

	if opts.Token == "" {
		return fmt.Errorf("worker join: no valid mTLS credentials found in %s and no --token provided", opts.TLSDir)
	}

	// ── Step 2: Setup worker node ─────────────────────────────────────────────
	if err := workerdeploy.Setup(ctx, rt, opts.Setup); err != nil {
		return fmt.Errorf("worker join: setup: %w", err)
	}

	// ── Step 3: Bootstrap — generate key+CSR, register, write certs ──────────
	workerName, err := bootstrap(ctx, opts.GatewayAddr, opts.Token, opts.TLSDir, rt.Type(), meta)
	if err != nil {
		return fmt.Errorf("worker join: bootstrap: %w", err)
	}
	logger.InfofCtx(ctx, "worker join: worker %q registered with control plane", workerName)

	// ── Step 4: Open CommandStream with mTLS ─────────────────────────────────
	return connectAndStream(ctx, rt, opts.GatewayAddr, opts.TLSDir, meta)
}

// ─── bootstrap ────────────────────────────────────────────────────────────────

// bootstrap generates a local ECDSA key + CSR, calls Register, writes the
// returned TLS material to tlsDir, and returns the worker name from the gateway.
func bootstrap(ctx context.Context, gatewayAddr, token, tlsDir string, rt types.RuntimeType, meta map[string]string) (string, error) {
	// 1. Generate local ECDSA P-256 private key — never transmitted.
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate key: %w", err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(privKey)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	// 2. Build CSR. The CN becomes the worker's cryptographic identity.
	hostname, _ := os.Hostname()
	csrTemplate := x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   hostname,
			Organization: []string{"system:workers"},
		},
		SignatureAlgorithm: x509.ECDSAWithSHA256,
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &csrTemplate, privKey)
	if err != nil {
		return "", fmt.Errorf("generate CSR: %w", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	// 3. Dial the gateway for bootstrap — TLS with optional server verification.
	//    ca.crt may not exist yet (first bootstrap), so fall back to InsecureSkipVerify.
	tlsCfg, err := buildTLSConfig(tlsDir, nil)
	if err != nil {
		return "", err
	}
	if tlsCfg.InsecureSkipVerify {
		logger.WarningfCtx(ctx, "worker join: ca.crt not present, bootstrap connection will use InsecureSkipVerify")
	}

	conn, err := grpc.NewClient(gatewayAddr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return "", fmt.Errorf("dial %s: %w", gatewayAddr, err)
	}
	defer conn.Close()

	// 4. Call Register with token + CSR.
	logger.InfolnCtx(ctx, "worker join: registering with catalog control plane...")
	resp, err := workerpb.NewWorkerGatewayClient(conn).Register(ctx, &workerpb.RegisterRequest{
		PreSharedToken: token,
		RuntimeType:    rt.String(),
		Metadata:       meta,
		CsrPem:         csrPEM,
	})
	if err != nil {
		return "", fmt.Errorf("Register RPC: %w", err)
	}

	// 5. Write TLS material to disk.
	if len(resp.GetTlsCertPem()) == 0 {
		return "", fmt.Errorf("gateway returned empty certificate — registration failed")
	}
	if err := writeTLSMaterial(tlsDir, resp.GetTlsCertPem(), keyPEM, resp.GetCaCertPem()); err != nil {
		return "", err
	}
	logger.InfofCtx(ctx, "worker join: mTLS credentials written to %s", tlsDir)

	return resp.GetWorkerName(), nil
}

// ─── credential check ─────────────────────────────────────────────────────────

// hasValidTLSCredentials returns true when tls.crt + tls.key exist in tlsDir,
// the cert has not expired, and the cert is signed by the stored ca.crt.
func hasValidTLSCredentials(ctx context.Context, tlsDir string) bool {
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(tlsDir, "tls.crt"),
		filepath.Join(tlsDir, "tls.key"),
	)
	if err != nil {
		return false
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return false
	}
	if !time.Now().Before(leaf.NotAfter) {
		return false
	}

	// Verify the client cert against the stored CA so we catch cases where
	// the CA was rotated and the on-disk cert is no longer trusted.
	caPath := filepath.Join(tlsDir, "ca.crt")
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No CA stored yet — key-pair alone is sufficient evidence.
			return true
		}
		logger.WarningfCtx(ctx, "worker join: credential check: read ca.crt: %v", err)
		return false
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		logger.WarningfCtx(ctx, "worker join: credential check: parse ca.crt failed")
		return false
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: pool})
	if err != nil {
		logger.WarningfCtx(ctx, "worker join: credential check: cert not trusted by stored CA: %v", err)
		return false
	}
	return true
}

// ─── TLS helpers ──────────────────────────────────────────────────────────────

// buildTLSConfig constructs a *tls.Config for connecting to the gateway.
// If ca.crt exists in tlsDir it is used for server verification; otherwise
// InsecureSkipVerify is set (bootstrap / first-contact scenario only).
func buildTLSConfig(tlsDir string, clientCert *tls.Certificate) (*tls.Config, error) {
	cfg := &tls.Config{}
	if clientCert != nil {
		cfg.Certificates = []tls.Certificate{*clientCert}
	}
	caPath := filepath.Join(tlsDir, "ca.crt")
	if _, err := os.Stat(caPath); err == nil {
		caPEM, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read ca.crt: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse ca.crt failed")
		}
		cfg.RootCAs = pool
		// ServerName must match the SAN in the gateway's server cert.
		// The gateway always generates the cert with GatewayServerName as the SAN,
		// so we set it here to satisfy Go's hostname verification — regardless of
		// the actual IP or DNS name used to dial the gateway.
		cfg.ServerName = workerconstants.GatewayServerName
	} else if os.IsNotExist(err) {
		cfg.InsecureSkipVerify = true //nolint:gosec // intentional bootstrap fallback
	} else {
		return nil, fmt.Errorf("stat ca.crt: %w", err)
	}
	return cfg, nil
}

// writeTLSMaterial writes cert, key, and optionally ca.crt to tlsDir.
func writeTLSMaterial(dir string, certPEM, keyPEM, caCertPEM []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), certPEM, 0o644); err != nil {
		return fmt.Errorf("write tls.crt: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), keyPEM, 0o600); err != nil {
		return fmt.Errorf("write tls.key: %w", err)
	}
	if len(caCertPEM) > 0 {
		if err := os.WriteFile(filepath.Join(dir, "ca.crt"), caCertPEM, 0o644); err != nil {
			return fmt.Errorf("write ca.crt: %w", err)
		}
	}
	return nil
}

// ─── stream loop ──────────────────────────────────────────────────────────────

// connectAndStream dials the gateway with mTLS credentials from tlsDir and
// runs the CommandStream retry loop until ctx is cancelled.
func connectAndStream(ctx context.Context, rt runtime.Runtime, gatewayAddr, tlsDir string, meta map[string]string) error {
	tlsCfg, err := buildTLSConfig(tlsDir, nil)
	if err != nil {
		return fmt.Errorf("worker join: build TLS config for stream: %w", err)
	}

	// Load the client cert for mTLS.
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(tlsDir, "tls.crt"),
		filepath.Join(tlsDir, "tls.key"),
	)
	if err != nil {
		return fmt.Errorf("worker join: load mTLS credentials: %w", err)
	}
	tlsCfg.Certificates = []tls.Certificate{cert}

	conn, err := grpc.NewClient(gatewayAddr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return fmt.Errorf("worker join: dial %s: %w", gatewayAddr, err)
	}
	defer conn.Close()

	// Derive worker name from the CN in the client cert.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("worker join: parse client cert: %w", err)
	}
	workerName := leaf.Subject.CommonName

	logger.InfofCtx(ctx, "worker join: connecting as %q to %s", workerName, gatewayAddr)

	client := workerpb.NewWorkerGatewayClient(conn)
	return runStreamLoop(ctx, rt, client, workerName)
}

// runStreamLoop opens the CommandStream and retries on transient failures.
func runStreamLoop(ctx context.Context, rt runtime.Runtime, client workerpb.WorkerGatewayClient, workerName string) error {
	backoff := retryBase

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		logger.InfofCtx(ctx, "worker join: opening CommandStream for %q...", workerName)

		err := runStream(ctx, rt, client, workerName)
		if err == nil || ctx.Err() != nil {
			return err
		}

		if status.Code(err) == codes.Unauthenticated {
			return fmt.Errorf("worker join: gateway rejected the stream — "+
				"re-run 'catalog worker register' and 'worker join' to reconnect: %w", err)
		}

		logger.WarningfCtx(ctx, "worker join: CommandStream disconnected (%v) — retrying in %s...", err, backoff)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*retryBackoffFactor, retryMax)
	}
}

// runStream opens one CommandStream, sends heartbeats, and drains Commands.
func runStream(ctx context.Context, rt runtime.Runtime, client workerpb.WorkerGatewayClient, workerName string) error {
	stream, err := client.CommandStream(ctx)
	if err != nil {
		return fmt.Errorf("open CommandStream: %w", err)
	}

	if err := sendHeartbeat(stream, workerName); err != nil {
		return fmt.Errorf("initial heartbeat: %w", err)
	}

	logger.InfofCtx(ctx, "worker join: CommandStream open for %q — press Ctrl-C to stop", workerName)

	recvErrCh := make(chan error, 1)
	go func() {
		recvErrCh <- recvLoop(ctx, rt, stream, workerName)
	}()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvErrCh:
			return err
		case <-ticker.C:
			if err := sendHeartbeat(stream, workerName); err != nil {
				return fmt.Errorf("heartbeat: %w", err)
			}
		}
	}
}

// recvLoop reads Commands, dispatches them, and sends results back.
func recvLoop(ctx context.Context, rt runtime.Runtime, stream grpc.BidiStreamingClient[workerpb.CommandResult, workerpb.Command], workerName string) error {
	for {
		cmd, err := stream.Recv()
		if err != nil {
			return err
		}
		logger.InfofCtx(ctx, "worker join: %q received command id=%s type=%s",
			workerName, cmd.GetCommandId(), cmd.GetType())

		result := dispatch.Dispatch(ctx, rt, cmd)
		result.WorkerName = workerName
		if err := stream.Send(result); err != nil {
			return fmt.Errorf("send result id=%s: %w", cmd.GetCommandId(), err)
		}
	}
}

func sendHeartbeat(stream grpc.BidiStreamingClient[workerpb.CommandResult, workerpb.Command], workerName string) error {
	return stream.Send(&workerpb.CommandResult{
		WorkerName:  workerName,
		IsHeartbeat: true,
	})
}
