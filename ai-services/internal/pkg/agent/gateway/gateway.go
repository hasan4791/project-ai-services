// Package gateway implements the AgentGateway gRPC server.
// It is co-located with the Catalog API Server (control plane) and listens
// on a separate port (default :9090) for bidirectional streams from worker
// agent daemons.
package gateway

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	agentpb "github.com/project-ai-services/ai-services/internal/pkg/agent/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/registry"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type contextKey string

const agentIDCtxKey contextKey = "agent_id"

// Gateway is the gRPC server that accepts connections from worker agents.
type Gateway struct {
	agentpb.UnimplementedAgentGatewayServer

	registry   *registry.Registry
	tokenStore *registry.TokenStore
	grpcServer *grpc.Server

	// Cryptographic materials for TLS and CSR signing
	caCert     *x509.Certificate
	caKey      interface{}
	serverCert tls.Certificate
	caCertPool *x509.CertPool
}

// New creates a Gateway backed by the given registry, token store, and crypto materials.
func New(reg *registry.Registry, ts *registry.TokenStore, caCert *x509.Certificate, caKey interface{}, serverCert tls.Certificate, caCertPool *x509.CertPool) *Gateway {
	return &Gateway{
		registry:   reg,
		tokenStore: ts,
		caCert:     caCert,
		caKey:      caKey,
		serverCert: serverCert,
		caCertPool: caCertPool,
	}
}

// Start begins listening on addr (e.g. ":9090") and serves gRPC in a background goroutine.
func (g *Gateway) Start(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("agent gateway: listen on %s: %w", addr, err)
	}

	// 1. Configure Hybrid TLS: Allow connections without client certs (for bootstrap)
	// but verify them rigorously if they are provided (for mTLS operations).
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{g.serverCert},
		ClientCAs:    g.caCertPool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}
	creds := credentials.NewTLS(tlsConfig)

	// 2. Register Interceptors to enforce mTLS rules at the application layer
	g.grpcServer = grpc.NewServer(
		grpc.Creds(creds),
		grpc.UnaryInterceptor(g.authUnaryInterceptor),
		grpc.StreamInterceptor(g.authStreamInterceptor),
	)

	agentpb.RegisterAgentGatewayServer(g.grpcServer, g)

	go func() {
		logger.InfofCtx(ctx, "AgentGateway gRPC server listening on %s", addr)
		if err := g.grpcServer.Serve(lis); err != nil {
			logger.ErrorfCtx(ctx, "AgentGateway gRPC server exited: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		logger.InfolnCtx(ctx, "AgentGateway shutting down")
		g.grpcServer.GracefulStop()
	}()

	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Interceptor Implementations (mTLS Enforcement)
// ──────────────────────────────────────────────────────────────────────────────

func (g *Gateway) authUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Bypass mTLS requirement ONLY for the Register endpoint
	// Note: Update this string to match your exact protobuf package/service name
	if info.FullMethod == "/agent.v1.AgentGateway/Register" || info.FullMethod == "/agent.AgentGateway/Register" {
		return handler(ctx, req)
	}

	agentID, err := extractAndVerifyAgentIdentity(ctx)
	if err != nil {
		return nil, err
	}

	// Inject cryptographically verified agent_id into context
	ctx = context.WithValue(ctx, agentIDCtxKey, agentID)
	return handler(ctx, req)
}

func (g *Gateway) authStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// All streams (CommandStream) strictly require mTLS
	agentID, err := extractAndVerifyAgentIdentity(ss.Context())
	if err != nil {
		return err
	}

	// Wrap stream to inject verified agent_id into context
	wrapped := &wrappedStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), agentIDCtxKey, agentID)}
	return handler(srv, wrapped)
}

func extractAndVerifyAgentIdentity(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return "", status.Error(codes.Unauthenticated, "no peer identity found")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return "", status.Error(codes.Unauthenticated, "mTLS client certificate required")
	}

	clientCert := tlsInfo.State.VerifiedChains[0][0]
	return clientCert.Subject.CommonName, nil
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

// ──────────────────────────────────────────────────────────────────────────────
// AgentGatewayServer implementation
// ──────────────────────────────────────────────────────────────────────────────

// Register implements AgentGatewayServer. Workers call this once at bootstrap.
func (g *Gateway) Register(ctx context.Context, req *agentpb.RegisterRequest) (*agentpb.RegisterResponse, error) {
	agentName := req.GetAgentName()
	logger.InfofCtx(ctx, "AgentGateway: Register request from agent_name=%s", agentName)

	// Validate the bootstrap token.
	if err := g.tokenStore.Validate(req.GetPreSharedToken()); err != nil {
		logger.WarningfCtx(ctx, "AgentGateway: rejected registration for %s: %v", agentName, err)
		return nil, fmt.Errorf("registration rejected: %w", err)
	}

	// 2. Parse and Validate the CSR
	block, _ := pem.Decode(req.GetCsrPem())
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, status.Error(codes.InvalidArgument, "malformed CSR format")
	}

	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to parse CSR: %v", err)
	}

	// 3. Security Guardrail: Prevent identity spoofing
	if csr.Subject.CommonName != agentName {
		return nil, status.Error(codes.InvalidArgument, "security violation: CSR CommonName does not match AgentID")
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid CSR signature")
	}

	// 4. Auto-Approve and Sign Certificate
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to generate certificate serial")
	}

	certTemplate := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      csr.Subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(1, 0, 0), // 1-year lifetime
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	signedCertBytes, err := x509.CreateCertificate(rand.Reader, certTemplate, g.caCert, csr.PublicKey, g.caKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to issue signed certificate: %v", err)
	}
	signedCertPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: signedCertBytes})

	// 5. Update Registry state
	entry, err := g.registry.Upsert(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert agent: %w", err)
	}
	g.registry.MarkReady(ctx, entry.AgentName)

	logger.InfofCtx(ctx, "AgentGateway: agent %s registered and marked READY", agentName)

	return &agentpb.RegisterResponse{
		AgentName:  agentName,
		TlsCertPem: signedCertPem,
	}, nil
}

// CommandStream implements AgentGatewayServer.
func (g *Gateway) CommandStream(stream grpc.BidiStreamingServer[agentpb.CommandResult, agentpb.Command]) error {
	ctx := stream.Context()

	// Receive the first message to identify the agent.
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("CommandStream: failed to receive first message: %w", err)
	}
	agentName := firstMsg.GetAgentName()
	if agentName == "" {
		return fmt.Errorf("CommandStream: first message missing agent_name")
	}

	entry, ok := g.registry.Get(agentName)
	if !ok {
		return fmt.Errorf("CommandStream: unknown agent %s – call Register first", agentName)
	}

	g.registry.MarkReady(ctx, agentName)
	logger.InfofCtx(ctx, "AgentGateway: CommandStream opened for agent %s", agentName)

	// Capture the worker's TCP source IP from the gRPC peer info so the
	// deployer can build the correct domain suffix for route registration.
	if p, ok := peer.FromContext(ctx); ok {
		if host, _, err := net.SplitHostPort(p.Addr.String()); err == nil && host != "" {
			g.registry.SetWorkerIP(agentName, host)
			logger.InfofCtx(ctx, "AgentGateway: agent %s peer IP = %s", agentName, host)
		}
	}

	// Capture the domain suffix the worker computed during 'agent configure'.
	// Sent via RegisterRequest.Labels["domain_suffix"].
	if entry, ok := g.registry.Get(agentName); ok {
		if suffix := entry.Labels["domain_suffix"]; suffix != "" {
			g.registry.SetDomainSuffix(agentName, suffix)
			logger.InfofCtx(ctx, "AgentGateway: agent %s domain suffix = %s", agentName, suffix)
		}
	}

	if !firstMsg.GetIsHeartbeat() {
		// Ensure agent_id is always correctly stamped
		firstMsg.AgentName = agentName
		g.registry.DeliverResult(firstMsg)
	} else {
		g.registry.UpdateHeartbeat(ctx, agentName)
	}

	// 3. goroutine: read results from the agent and dispatch
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			// Ensure agent_name is always stamped on the result.
			if res.AgentName == "" {
				res.AgentName = agentName
			}
			if res.GetIsHeartbeat() {
				g.registry.UpdateHeartbeat(ctx, agentName)
				continue
			}
			g.registry.DeliverResult(res)
		}
	}()

	// 4. Main loop: pull Commands from the agent's CommandCh
	for {
		select {
		case <-ctx.Done():
			g.registry.MarkDisconnected(context.Background(), agentName)
			logger.InfofCtx(ctx, "AgentGateway: context done for agent %s", agentName)
			return ctx.Err()

		case err := <-recvErrCh:
			g.registry.MarkDisconnected(context.Background(), agentName)
			logger.InfofCtx(ctx, "AgentGateway: agent %s disconnected: %v", agentName, err)
			return err

		case cmd, ok := <-entry.CommandCh:
			if !ok {
				return fmt.Errorf("CommandStream: command channel closed for agent %s", agentName)
			}
			if err := stream.Send(cmd); err != nil {
				g.registry.MarkDisconnected(context.Background(), agentName)
				return fmt.Errorf("CommandStream: send to agent %s: %w", agentName, err)
			}
		}
	}
}
