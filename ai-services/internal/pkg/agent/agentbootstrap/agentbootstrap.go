// Package agentbootstrap handles agent-side registration with the control plane.
// It calls AgentGateway.Register with the parameters provided at agent start,
// and writes any returned TLS credentials to tlsDir (reserved for future mTLS).
package agentbootstrap

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	agentpb "github.com/project-ai-services/ai-services/internal/pkg/agent/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
)

const (
	// DefaultAgentTLSDir is where mTLS cert/key will be written in a future release.
	DefaultAgentTLSDir = "/etc/ai-services/agent-tls"
)

// Config holds the parameters required to register an agent with the control plane.
type Config struct {
	ControlPlaneURL string            // e.g. "lpar-0.example.com:9090"
	AgentName       string            // human-readable name for this worker
	PreSharedToken  string            // single-use bootstrap token issued via catalog agent issue-token
	Runtime         string            // "podman" (default) or "openshift"
	Labels          map[string]string
	Capabilities    map[string]string
}

// Register calls AgentGateway.Register using the provided config.
// On success it writes any returned TLS material to tlsDir (for future mTLS).
func Register(ctx context.Context, cfg Config, tlsDir string) error {
	conn, err := grpc.NewClient(cfg.ControlPlaneURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("agentbootstrap: dial %s: %w", cfg.ControlPlaneURL, err)
	}
	defer conn.Close()

	client := agentpb.NewAgentGatewayClient(conn)
	resp, err := client.Register(ctx, &agentpb.RegisterRequest{
		AgentName:      cfg.AgentName,
		PreSharedToken: cfg.PreSharedToken,
		Labels:         cfg.Labels,
		Capabilities:   cfg.Capabilities,
	})
	if err != nil {
		return fmt.Errorf("agentbootstrap: Register RPC failed: %w", err)
	}

	logger.InfofCtx(ctx, "agentbootstrap: registered as agent_name=%s", resp.GetAgentName())

	// Write TLS credentials if provided (future mTLS path).
	if resp.GetTlsCertPem() != "" && resp.GetTlsKeyPem() != "" {
		if err := writeTLSMaterial(tlsDir, resp.GetTlsCertPem(), resp.GetTlsKeyPem()); err != nil {
			return err
		}
		logger.InfofCtx(ctx, "agentbootstrap: TLS material written to %s", tlsDir)
	}

	return nil
}

func writeTLSMaterial(dir, certPEM, keyPEM string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("agentbootstrap: mkdir %s: %w", dir, err)
	}
	if err := os.WriteFile(dir+"/tls.crt", []byte(certPEM), 0o600); err != nil {
		return fmt.Errorf("agentbootstrap: write cert: %w", err)
	}
	if err := os.WriteFile(dir+"/tls.key", []byte(keyPEM), 0o600); err != nil {
		return fmt.Errorf("agentbootstrap: write key: %w", err)
	}
	return nil
}
