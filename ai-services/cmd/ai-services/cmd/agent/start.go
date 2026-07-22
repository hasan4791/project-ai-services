package agent

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/project-ai-services/ai-services/internal/pkg/agent/agentbootstrap"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/agentconfig"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/daemon"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime"
	openshiftRuntime "github.com/project-ai-services/ai-services/internal/pkg/runtime/openshift"
	podmanRuntime "github.com/project-ai-services/ai-services/internal/pkg/runtime/podman"
)

func newStartCmd() *cobra.Command {
	var (
		server       string
		agentName    string
		token        string
		runtimeName  string
		tlsDir       = agentbootstrap.DefaultAgentTLSDir
	)

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Register with the control plane and start the agent daemon",
		Long: `Register this Worker LPAR with the control-plane AgentGateway then start
the persistent bidirectional gRPC CommandStream daemon.

Steps performed:
  1. Calls AgentGateway.Register using the provided --token
  2. Loops forever on the CommandStream, executing runtime commands
     on behalf of the control plane

Prerequisites (run once before this command):
  - ai-services bootstrap configure --runtime podman
  - Obtain a token:  ai-services catalog agent issue-token   (on control plane)

Run as a systemd service for production use.`,
		Example: `  ai-services agent start --server lpar-0.example.com:9090 --name lpar-1 --token <token>
  ai-services agent start --server lpar-0.example.com:9090 --name lpar-1 --token <token> --runtime openshift`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			if server == "" {
				return fmt.Errorf("--server is required (e.g. lpar-0.example.com:9090)")
			}
			if agentName == "" {
				return fmt.Errorf("--name is required")
			}
			if token == "" {
				return fmt.Errorf("--token is required (obtain via: ai-services catalog agent issue-token)")
			}
			return runStart(server, agentName, token, runtimeName, tlsDir)
		},
	}

	cmd.Flags().StringVar(&server, "server", "", "Control-plane AgentGateway address (host:port)")
	cmd.Flags().StringVar(&agentName, "name", "", "Name to register this agent under")
	cmd.Flags().StringVar(&token, "token", "", "Single-use bootstrap token (from: ai-services catalog agent issue-token)")
	cmd.Flags().StringVar(&runtimeName, "runtime", "podman", "Local runtime to use: podman or openshift")
	cmd.Flags().StringVar(&tlsDir, "tls-dir", tlsDir, "Directory to write TLS material (future mTLS)")

	return cmd
}

func runStart(server, agentName, token, runtimeName, tlsDir string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := agentbootstrap.Config{
		ControlPlaneURL: server,
		AgentName:       agentName,
		PreSharedToken:  token,
	}

	if err := agentbootstrap.Register(ctx, cfg, tlsDir); err != nil {
		return fmt.Errorf("agent start: registration failed: %w", err)
	}

	// Persist agent name and server so `agent status` can work without flags.
	if err := agentconfig.Save(agentconfig.AgentConfig{
		AgentName: agentName,
		Server:    server,
	}); err != nil {
		// Non-fatal: warn but don't abort the daemon.
		logger.Warningf("agent start: could not save agent config: %v\n", err)
	}

	rt, err := buildRuntime(runtimeName)
	if err != nil {
		return fmt.Errorf("agent start: %w", err)
	}

	logger.Infoln("Agent daemon running. Press Ctrl+C to stop.")

	return daemon.New(daemon.Config{
		AgentName:       agentName,
		ControlPlaneURL: server,
		PreSharedToken:  token,
	}, rt).Run(ctx)
}

// buildRuntime constructs the local runtime for the given name.
func buildRuntime(name string) (runtime.Runtime, error) {
	switch name {
	case "podman":
		rt, err := podmanRuntime.NewPodmanClient()
		if err != nil {
			return nil, fmt.Errorf("failed to initialise Podman runtime: %w", err)
		}
		return rt, nil
	case "openshift":
		rt, err := openshiftRuntime.NewOpenshiftClientWithNamespace("")
		if err != nil {
			return nil, fmt.Errorf("failed to initialise OpenShift runtime: %w", err)
		}
		return rt, nil
	default:
		return nil, fmt.Errorf("unknown runtime %q: must be 'podman' or 'openshift'", name)
	}
}
