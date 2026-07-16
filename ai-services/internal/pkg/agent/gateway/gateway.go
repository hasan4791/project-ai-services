// Package gateway implements the AgentGateway gRPC server.
// It is co-located with the Catalog API Server (control plane) and listens
// on a separate port (default :9090) for bidirectional streams from worker
// agent daemons.
package gateway

import (
	"context"
	"fmt"
	"net"

	agentpb "github.com/project-ai-services/ai-services/internal/pkg/agent/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/registry"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	"google.golang.org/grpc"
)

// Gateway is the gRPC server that accepts connections from worker agents.
type Gateway struct {
	agentpb.UnimplementedAgentGatewayServer

	registry   *registry.Registry
	tokenStore *registry.TokenStore
	grpcServer *grpc.Server
}

// New creates a Gateway backed by the given registry and token store.
func New(reg *registry.Registry, ts *registry.TokenStore) *Gateway {
	return &Gateway{
		registry:   reg,
		tokenStore: ts,
	}
}

// Start begins listening on addr (e.g. ":9090") and serves gRPC in a background goroutine.
// The server shuts down gracefully when ctx is cancelled.
func (g *Gateway) Start(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("agent gateway: listen on %s: %w", addr, err)
	}

	g.grpcServer = grpc.NewServer()
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
// AgentGatewayServer implementation
// ──────────────────────────────────────────────────────────────────────────────

// Register implements AgentGatewayServer. Workers call this once at bootstrap.
func (g *Gateway) Register(ctx context.Context, req *agentpb.RegisterRequest) (*agentpb.RegisterResponse, error) {
	agentID := req.GetAgentId()
	logger.InfofCtx(ctx, "AgentGateway: Register request from agent_id=%s", agentID)

	// Validate the bootstrap token.
	boundAgentID, err := g.tokenStore.Validate(req.GetPreSharedToken())
	if err != nil {
		logger.WarningfCtx(ctx, "AgentGateway: rejected registration for %s: %v", agentID, err)
		return nil, fmt.Errorf("registration rejected: %w", err)
	}
	if boundAgentID != agentID {
		return nil, fmt.Errorf("registration rejected: token not issued for agent_id %s", agentID)
	}

	entry, err := g.registry.Upsert(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert agent: %w", err)
	}
	g.registry.MarkReady(ctx, entry.AgentID)

	logger.InfofCtx(ctx, "AgentGateway: agent %s registered and marked READY", agentID)

	return &agentpb.RegisterResponse{
		AgentId: agentID,
		// TlsCertPem / TlsKeyPem intentionally empty; mTLS added in a future iteration.
	}, nil
}

// CommandStream implements AgentGatewayServer.
// The agent initiates the bidirectional stream. This method:
//  1. Reads the first CommandResult to identify which agent connected.
//  2. Routes incoming results to the waiting RemoteRuntime callers.
//  3. Drains the agent's CommandCh and writes Commands to the stream.
func (g *Gateway) CommandStream(stream grpc.BidiStreamingServer[agentpb.CommandResult, agentpb.Command]) error {
	ctx := stream.Context()

	// Receive the first message to identify the agent.
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("CommandStream: failed to receive first message: %w", err)
	}
	agentID := firstMsg.GetAgentId()
	if agentID == "" {
		return fmt.Errorf("CommandStream: first message missing agent_id")
	}

	entry, ok := g.registry.Get(agentID)
	if !ok {
		return fmt.Errorf("CommandStream: unknown agent_id %s – call Register first", agentID)
	}

	g.registry.MarkReady(ctx, agentID)
	logger.InfofCtx(ctx, "AgentGateway: CommandStream opened for agent %s", agentID)

	// Deliver the first message if it is a real result (not a heartbeat).
	if !firstMsg.GetIsHeartbeat() {
		g.registry.DeliverResult(firstMsg)
	} else {
		g.registry.UpdateHeartbeat(ctx, agentID)
	}

	// goroutine: read results from the agent and dispatch to waiting callers.
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			// Ensure agent_id is always stamped on the result.
			if res.AgentId == "" {
				res.AgentId = agentID
			}
			if res.GetIsHeartbeat() {
				g.registry.UpdateHeartbeat(ctx, agentID)
				continue
			}
			g.registry.DeliverResult(res)
		}
	}()

	// Main loop: pull Commands from the agent's CommandCh and send them downstream.
	for {
		select {
		case <-ctx.Done():
			g.registry.MarkDisconnected(context.Background(), agentID)
			logger.InfofCtx(ctx, "AgentGateway: context done for agent %s", agentID)
			return ctx.Err()

		case err := <-recvErrCh:
			g.registry.MarkDisconnected(context.Background(), agentID)
			logger.InfofCtx(ctx, "AgentGateway: agent %s disconnected: %v", agentID, err)
			return err

		case cmd, ok := <-entry.CommandCh:
			if !ok {
				return fmt.Errorf("CommandStream: command channel closed for agent %s", agentID)
			}
			if err := stream.Send(cmd); err != nil {
				g.registry.MarkDisconnected(context.Background(), agentID)
				return fmt.Errorf("CommandStream: send to agent %s: %w", agentID, err)
			}
		}
	}
}
