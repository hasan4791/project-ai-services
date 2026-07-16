// Package agentbootstrap handles agent-side registration with the control plane.
// It reads /etc/ai-services/agent.conf, calls AgentGateway.Register, and writes
// the returned TLS credentials to /etc/ai-services/agent-tls/ (reserved for
// future mTLS implementation).
package agentbootstrap

import (
	"context"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	agentpb "github.com/project-ai-services/ai-services/internal/pkg/agent/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
)

const (
	// DefaultAgentConfPath is the canonical location of the agent config file.
	DefaultAgentConfPath = "/etc/ai-services/agent.conf"
	// DefaultAgentTLSDir is where mTLS cert/key will be written in a future release.
	DefaultAgentTLSDir = "/etc/ai-services/agent-tls"
)

// AgentConf is the on-disk configuration written by an admin before bootstrapping.
type AgentConf struct {
	ControlPlaneURL string            `yaml:"control_plane_url"` // e.g. "lpar-0.example.com:9090"
	AgentID         string            `yaml:"agent_id"`
	PreSharedToken  string            `yaml:"pre_shared_token"`
	Labels          map[string]string `yaml:"labels"`
	Capabilities    map[string]string `yaml:"capabilities"`
}

// LoadConf reads and parses the agent configuration from path.
func LoadConf(path string) (*AgentConf, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("agentbootstrap: read %s: %w", path, err)
	}
	var conf AgentConf
	if err := yaml.Unmarshal(data, &conf); err != nil {
		return nil, fmt.Errorf("agentbootstrap: parse %s: %w", path, err)
	}
	if conf.ControlPlaneURL == "" {
		return nil, fmt.Errorf("agentbootstrap: control_plane_url is required in %s", path)
	}
	if conf.AgentID == "" {
		return nil, fmt.Errorf("agentbootstrap: agent_id is required in %s", path)
	}
	if conf.PreSharedToken == "" {
		return nil, fmt.Errorf("agentbootstrap: pre_shared_token is required in %s", path)
	}
	return &conf, nil
}

// Register calls AgentGateway.Register using the configuration at confPath.
// On success it writes any returned TLS material to tlsDir (for future mTLS).
func Register(ctx context.Context, confPath, tlsDir string) (*AgentConf, error) {
	conf, err := LoadConf(confPath)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(conf.ControlPlaneURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("agentbootstrap: dial %s: %w", conf.ControlPlaneURL, err)
	}
	defer conn.Close()

	client := agentpb.NewAgentGatewayClient(conn)
	resp, err := client.Register(ctx, &agentpb.RegisterRequest{
		AgentId:        conf.AgentID,
		PreSharedToken: conf.PreSharedToken,
		Labels:         conf.Labels,
		Capabilities:   conf.Capabilities,
	})
	if err != nil {
		return nil, fmt.Errorf("agentbootstrap: Register RPC failed: %w", err)
	}

	logger.InfofCtx(ctx, "agentbootstrap: registered as agent_id=%s", resp.GetAgentId())

	// Write TLS credentials if provided (future mTLS path).
	if resp.GetTlsCertPem() != "" && resp.GetTlsKeyPem() != "" {
		if err := writeTLSMaterial(tlsDir, resp.GetTlsCertPem(), resp.GetTlsKeyPem()); err != nil {
			return nil, err
		}
		logger.InfofCtx(ctx, "agentbootstrap: TLS material written to %s", tlsDir)
	}

	return conf, nil
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
