// Package gateway implements the WorkerGateway gRPC server.
// It is co-located with the Catalog API Server (control plane) and listens
// on a separate port (default :9090) for bidirectional streams from worker
// daemons.
//
// TLS model (post-mTLS upgrade):
//   - The gateway generates (or loads) a self-signed CA and server cert on first
//     start, persisting them under DefaultPKIDir.
//   - ClientAuth is set to VerifyClientCertIfGiven so workers can call Register
//     without a client cert. The authStreamInterceptor then enforces that
//     CommandStream connections carry a valid, CA-signed client certificate.
package gateway

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	workerpb "github.com/project-ai-services/ai-services/internal/pkg/worker/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/worker/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	// DefaultPKIDir is where gateway PKI files are persisted on the control plane.
	DefaultPKIDir = "/var/lib/ai-services/gateway-pki"

	// GatewayServerName is the fixed DNS SAN embedded in the auto-generated server
	// certificate. Workers set tls.Config.ServerName to this value so hostname
	// verification succeeds regardless of the IP or public DNS name used to reach
	// the gateway.
	GatewayServerName = "worker-gateway.ai-services.internal"

	// heartbeatTimeout is the maximum time since the last recorded heartbeat
	// before a worker is considered disconnected.
	heartbeatTimeout = 90 * time.Second

	// sweepInterval is how often the background sweeper checks for stale workers.
	sweepInterval = 30 * time.Second
)

// Gateway is the gRPC server that accepts connections from workers.
type Gateway struct {
	workerpb.UnimplementedWorkerGatewayServer

	registry   *registry.Registry
	grpcServer *grpc.Server

	// PKI material — loaded or generated from pkiDir on first start.
	caCert     *x509.Certificate
	caKey      *ecdsa.PrivateKey
	serverCert tls.Certificate
	caCertPool *x509.CertPool
}

// New creates a Gateway backed by the given registry.
// It calls loadOrGeneratePKI(pkiDir) to load or auto-generate the control-plane
// CA and server certificate. If pkiDir is empty, DefaultPKIDir is used.
func New(ctx context.Context, reg *registry.Registry, pkiDir string) (*Gateway, error) {
	if pkiDir == "" {
		pkiDir = DefaultPKIDir
	}
	caCert, caKey, serverCert, caCertPool, err := loadOrGeneratePKI(ctx, pkiDir)
	if err != nil {
		return nil, fmt.Errorf("worker gateway: PKI init failed: %w", err)
	}
	return &Gateway{
		registry:   reg,
		caCert:     caCert,
		caKey:      caKey,
		serverCert: serverCert,
		caCertPool: caCertPool,
	}, nil
}

// Start begins listening on addr (e.g. ":9090") and serves gRPC in a background goroutine.
// It also starts the heartbeat sweeper. Both stop when ctx is cancelled.
// cancel is a CancelCauseFunc for the server's root context; it is called with the
// Serve error if the gRPC listener fails unexpectedly, so the whole process shuts down cleanly.
func (g *Gateway) Start(ctx context.Context, cancel context.CancelCauseFunc, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("worker gateway: listen on %s: %w", addr, err)
	}

	// Hybrid TLS: allow connections without client certs (for bootstrap Register)
	// but verify them rigorously if they are provided (for mTLS CommandStream).
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{g.serverCert},
		ClientCAs:    g.caCertPool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}

	g.grpcServer = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.UnaryInterceptor(g.authUnaryInterceptor),
		grpc.StreamInterceptor(g.authStreamInterceptor),
	)
	workerpb.RegisterWorkerGatewayServer(g.grpcServer, g)

	go func() {
		logger.InfofCtx(ctx, "WorkerGateway gRPC server listening on %s", addr)
		if err := g.grpcServer.Serve(lis); err != nil {
			logger.ErrorfCtx(ctx, "WorkerGateway gRPC server failed: %v", err)
			cancel(fmt.Errorf("worker gateway: gRPC server failed: %w", err))
		}
	}()

	go g.runSweeper(ctx)

	go func() {
		<-ctx.Done()
		logger.InfolnCtx(ctx, "WorkerGateway shutting down")
		g.grpcServer.GracefulStop()
	}()

	return nil
}

// runSweeper periodically asks the registry to mark stale workers disconnected.
func (g *Gateway) runSweeper(ctx context.Context) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.registry.SweepStale(ctx, heartbeatTimeout)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// PKI bootstrap
// ──────────────────────────────────────────────────────────────────────────────

// loadOrGeneratePKI loads the four PKI files from pkiDir if they all exist, or
// generates a new ECDSA P-256 root CA and server certificate on first start.
func loadOrGeneratePKI(ctx context.Context, pkiDir string) (*x509.Certificate, *ecdsa.PrivateKey, tls.Certificate, *x509.CertPool, error) {
	caKeyPath := filepath.Join(pkiDir, "ca.key")
	caCrtPath := filepath.Join(pkiDir, "ca.crt")
	srvKeyPath := filepath.Join(pkiDir, "server.key")
	srvCrtPath := filepath.Join(pkiDir, "server.crt")

	if fileExists(caKeyPath) && fileExists(caCrtPath) && fileExists(srvKeyPath) && fileExists(srvCrtPath) {
		logger.InfofCtx(ctx, "worker gateway: PKI files found in %s, loading existing material", pkiDir)
		return loadPKI(caCrtPath, caKeyPath, srvCrtPath, srvKeyPath)
	}

	logger.InfofCtx(ctx, "worker gateway: PKI directory empty or incomplete — generating new CA and server certificate in %s", pkiDir)
	return generateAndPersistPKI(ctx, pkiDir)
}

// generateAndPersistPKI creates a new ECDSA P-256 root CA (TTL 10 years) and
// signs a server certificate (TTL 1 year), then writes all four files to pkiDir.
// The server cert carries GatewayServerName as its DNS SAN so workers can verify
// the server without knowing the gateway's actual public hostname or IP.
func generateAndPersistPKI(ctx context.Context, pkiDir string) (*x509.Certificate, *ecdsa.PrivateKey, tls.Certificate, *x509.CertPool, error) {
	if err := os.MkdirAll(pkiDir, 0o700); err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("mkdir %s: %w", pkiDir, err)
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("generate CA key: %w", err)
	}

	caSerial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "ai-services-worker-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("sign CA cert: %w", err)
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("parse CA cert: %w", err)
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("generate server key: %w", err)
	}
	srvSerial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	srvTemplate := &x509.Certificate{
		SerialNumber: srvSerial,
		Subject:      pkix.Name{CommonName: "WorkerGateway"},
		DNSNames:     []string{GatewayServerName},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	srvCertDER, err := x509.CreateCertificate(rand.Reader, srvTemplate, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("sign server cert: %w", err)
	}

	caKeyDER, _ := x509.MarshalECPrivateKey(caKey)
	srvKeyDER, _ := x509.MarshalECPrivateKey(srvKey)

	files := []struct {
		path string
		perm os.FileMode
		data []byte
	}{
		{filepath.Join(pkiDir, "ca.key"), 0o600, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyDER})},
		{filepath.Join(pkiDir, "ca.crt"), 0o644, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})},
		{filepath.Join(pkiDir, "server.key"), 0o600, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER})},
		{filepath.Join(pkiDir, "server.crt"), 0o644, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvCertDER})},
	}
	for _, f := range files {
		if err := os.WriteFile(f.path, f.data, f.perm); err != nil {
			return nil, nil, tls.Certificate{}, nil, fmt.Errorf("write %s: %w", f.path, err)
		}
	}
	logger.InfofCtx(ctx, "worker gateway: PKI generated and persisted to %s", pkiDir)

	serverCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvCertDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER}),
	)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("build server tls.Certificate: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return caCert, caKey, serverCert, pool, nil
}

// loadPKI reads all four PKI files from disk and returns the parsed material.
func loadPKI(caCrtPath, caKeyPath, srvCrtPath, srvKeyPath string) (*x509.Certificate, *ecdsa.PrivateKey, tls.Certificate, *x509.CertPool, error) {
	caCertPEM, err := os.ReadFile(caCrtPath)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("read %s: %w", caCrtPath, err)
	}
	block, _ := pem.Decode(caCertPEM)
	if block == nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("decode %s: not valid PEM", caCrtPath)
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("parse %s: %w", caCrtPath, err)
	}

	caKeyPEM, err := os.ReadFile(caKeyPath)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("read %s: %w", caKeyPath, err)
	}
	keyBlock, _ := pem.Decode(caKeyPEM)
	if keyBlock == nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("decode %s: not valid PEM", caKeyPath)
	}
	caKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("parse %s: %w", caKeyPath, err)
	}

	srvCertPEM, err := os.ReadFile(srvCrtPath)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("read %s: %w", srvCrtPath, err)
	}
	srvKeyPEM, err := os.ReadFile(srvKeyPath)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("read %s: %w", srvKeyPath, err)
	}
	serverCert, err := tls.X509KeyPair(srvCertPEM, srvKeyPEM)
	if err != nil {
		return nil, nil, tls.Certificate{}, nil, fmt.Errorf("load server key pair: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return caCert, caKey, serverCert, pool, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Interceptors (mTLS enforcement)
// ──────────────────────────────────────────────────────────────────────────────

// authUnaryInterceptor enforces that non-Register unary RPCs carry a valid
// CA-signed client certificate. Register is exempt — it uses token auth and
// runs before the worker has a cert.
func (g *Gateway) authUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if info.FullMethod == workerpb.WorkerGateway_Register_FullMethodName {
		return handler(ctx, req)
	}
	if err := requireClientCert(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// authStreamInterceptor enforces that CommandStream connections carry a valid
// CA-signed client certificate. Register is unary and goes through authUnaryInterceptor,
// so this interceptor can unconditionally require a verified client cert.
func (g *Gateway) authStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := requireClientCert(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

// requireClientCert checks that the context contains a peer with a CA-verified
// client certificate. Returns codes.Unauthenticated if the check fails.
func requireClientCert(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return status.Error(codes.Unauthenticated, "no peer identity found")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return status.Error(codes.Unauthenticated, "mTLS client certificate required")
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// WorkerGatewayServer implementation
// ──────────────────────────────────────────────────────────────────────────────

// Register implements WorkerGatewayServer. Workers call this once at bootstrap.
// The worker name is taken from the validated token (authoritative, not the request).
// If the request contains a CSR, it is signed and the cert + CA are returned for mTLS.
func (g *Gateway) Register(ctx context.Context, req *workerpb.RegisterRequest) (*workerpb.RegisterResponse, error) {
	peerAddr := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		peerAddr = p.Addr.String()
	}

	// 1. Validate token — worker name is bound to the token, not the request.
	workerName, err := g.registry.ValidateToken(req.GetPreSharedToken())
	if err != nil {
		logger.WarningfCtx(ctx, "WorkerGateway: rejected registration from %s: %v", peerAddr, err)
		return nil, status.Errorf(codes.Unauthenticated, "registration rejected: %v", err)
	}

	// 2. Parse, validate, and sign the CSR if provided.
	var tlsCertPEM, caCertPEM []byte
	if csrPEM := req.GetCsrPem(); len(csrPEM) > 0 {
		block, _ := pem.Decode(csrPEM)
		if block == nil || block.Type != "CERTIFICATE REQUEST" {
			logger.WarningfCtx(ctx, "WorkerGateway: malformed CSR from %s (worker=%s)", peerAddr, workerName)
			return nil, status.Error(codes.InvalidArgument, "malformed CSR format")
		}
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil {
			logger.WarningfCtx(ctx, "WorkerGateway: CSR parse error from %s (worker=%s): %v", peerAddr, workerName, err)
			return nil, status.Errorf(codes.InvalidArgument, "failed to parse CSR: %v", err)
		}
		if err := csr.CheckSignature(); err != nil {
			logger.WarningfCtx(ctx, "WorkerGateway: invalid CSR signature from %s (worker=%s): %v", peerAddr, workerName, err)
			return nil, status.Error(codes.InvalidArgument, "invalid CSR signature")
		}

		serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		notAfter := time.Now().AddDate(1, 0, 0)
		certTemplate := &x509.Certificate{
			SerialNumber: serial,
			Subject:      csr.Subject,
			NotBefore:    time.Now(),
			NotAfter:     notAfter,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			KeyUsage:     x509.KeyUsageDigitalSignature,
		}
		signedDER, err := x509.CreateCertificate(rand.Reader, certTemplate, g.caCert, csr.PublicKey, g.caKey)
		if err != nil {
			logger.ErrorfCtx(ctx, "WorkerGateway: cert signing failed for %s: %v", workerName, err)
			return nil, status.Errorf(codes.Internal, "failed to sign certificate: %v", err)
		}
		tlsCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: signedDER})
		caCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: g.caCert.Raw})
		logger.InfofCtx(ctx, "WorkerGateway: worker %q registered from %s, cert valid until %s",
			workerName, peerAddr, notAfter.UTC().Format("2006-01-02"))
	} else {
		logger.InfofCtx(ctx, "WorkerGateway: worker %q registered from %s (no CSR — plaintext mode)", workerName, peerAddr)
	}

	// 3. Register in-memory and persist to DB.
	if _, err := g.registry.Register(ctx, workerName, req.GetRuntimeType(), req.GetMetadata()); err != nil {
		if errors.Is(err, registry.ErrWorkerAlreadyActive) {
			return nil, status.Errorf(codes.AlreadyExists, "worker %s is already active", workerName)
		}
		return nil, fmt.Errorf("failed to register worker: %w", err)
	}

	return &workerpb.RegisterResponse{
		WorkerName: workerName,
		TlsCertPem: tlsCertPEM,
		CaCertPem:  caCertPEM,
	}, nil
}

// CommandStream implements WorkerGatewayServer.
func (g *Gateway) CommandStream(stream grpc.BidiStreamingServer[workerpb.CommandResult, workerpb.Command]) error { //nolint:gocognit
	ctx := stream.Context()

	workerName, entry, err := g.identifyWorker(ctx, stream)
	if err != nil {
		return err
	}

	recvErrCh := make(chan error, 1)
	go g.recvLoop(ctx, stream, workerName, recvErrCh)

	for {
		select {
		case <-ctx.Done():
			g.registry.Disconnect(context.Background(), workerName)
			logger.InfofCtx(ctx, "WorkerGateway: context done for worker %s", workerName)
			return ctx.Err()

		case err := <-recvErrCh:
			g.registry.Disconnect(context.Background(), workerName)
			logger.WarningfCtx(ctx, "WorkerGateway: worker %q stream closed: %v", workerName, err)
			return err

		case cmd, ok := <-entry.CommandCh:
			if !ok {
				return fmt.Errorf("CommandStream: command channel closed for worker %s", workerName)
			}
			if err := stream.Send(cmd); err != nil {
				g.registry.Disconnect(context.Background(), workerName)
				return fmt.Errorf("CommandStream: send to worker %s: %w", workerName, err)
			}
		}
	}
}

// identifyWorker reads the first message from the stream, validates the worker
// is known, and returns the worker name and registry entry.
//
// Error codes used by the worker daemon to decide its retry strategy:
//   - codes.Unauthenticated — worker not in registry; must call Register before retrying CommandStream.
//   - codes.InvalidArgument  — first message is malformed; worker has a bug.
//   - any other error        — transient; retry CommandStream with backoff (no re-registration needed).
func (g *Gateway) identifyWorker(ctx context.Context, stream grpc.BidiStreamingServer[workerpb.CommandResult, workerpb.Command]) (string, *registry.WorkerEntry, error) {
	firstMsg, err := stream.Recv()
	if err != nil {
		return "", nil, fmt.Errorf("CommandStream: failed to receive first message: %w", err)
	}
	workerName := firstMsg.GetWorkerName()
	if workerName == "" {
		return "", nil, status.Error(codes.InvalidArgument, "CommandStream: first message missing worker_name")
	}

	entry, ok := g.registry.Get(workerName)
	if !ok {
		return "", nil, status.Errorf(codes.Unauthenticated,
			"CommandStream: worker %s not registered — call Register first", workerName)
	}

	logger.InfofCtx(ctx, "WorkerGateway: CommandStream opened for worker %s", workerName)

	if firstMsg.GetIsHeartbeat() {
		g.registry.UpdateHeartbeat(ctx, workerName)
	} else {
		g.registry.DeliverResult(firstMsg)
	}

	return workerName, entry, nil
}

func (g *Gateway) recvLoop(ctx context.Context, stream grpc.BidiStreamingServer[workerpb.CommandResult, workerpb.Command], workerName string, errCh chan<- error) {
	for {
		res, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		if res.WorkerName == "" {
			res.WorkerName = workerName
		}
		if res.GetIsHeartbeat() {
			g.registry.UpdateHeartbeat(ctx, workerName)
			continue
		}
		g.registry.DeliverResult(res)
	}
}
