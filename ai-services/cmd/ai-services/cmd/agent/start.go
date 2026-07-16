package agent

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/project-ai-services/ai-services/internal/pkg/agent/agentbootstrap"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/daemon"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
)

func newStartCmd() *cobra.Command {
	var (
		confPath = agentbootstrap.DefaultAgentConfPath
		tlsDir   = agentbootstrap.DefaultAgentTLSDir
	)

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Register with the control plane and start the agent daemon",
		Long: `Register this Worker LPAR with the control-plane AgentGateway then start
the persistent bidirectional gRPC CommandStream daemon.

This is the single command that turns a bootstrapped Worker LPAR into an
active remote worker. It performs these steps in sequence:

  1. Reads /etc/ai-services/agent.conf (written by the admin)
  2. Calls AgentGateway.Register using the pre_shared_token
  3. Loops forever on the CommandStream, executing Podman commands
     on behalf of the control plane

Prerequisites (run once before this command):
  - ai-services bootstrap configure --runtime podman
  - Write /etc/ai-services/agent.conf with control_plane_url, agent_id,
    and a pre_shared_token issued via:
      ai-services catalog agent issue-token <agent-id>   (on the control plane)

Run as a systemd service for production use.`,
		Example: `  ai-services agent start
  ai-services agent start --conf /etc/ai-services/agent.conf`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			return runStart(confPath, tlsDir)
		},
	}

	cmd.Flags().StringVar(&confPath, "conf", confPath, "Path to the agent configuration file")
	cmd.Flags().StringVar(&tlsDir, "tls-dir", tlsDir, "Directory to write TLS material (future mTLS)")

	return cmd
}

func runStart(confPath, tlsDir string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Load config — agent.conf must exist and be populated by the admin.
	conf, err := agentbootstrap.LoadConf(confPath)
	if err != nil {
		return fmt.Errorf("agent start: %w\n\n"+
			"Ensure %s contains control_plane_url, agent_id, and pre_shared_token.\n"+
			"Obtain a token via: ai-services catalog agent issue-token <agent-id>",
			err, confPath)
	}

	// Register with the control plane. Writes TLS material to tlsDir if returned.
	if _, err := agentbootstrap.Register(ctx, confPath, tlsDir); err != nil {
		return fmt.Errorf("agent start: registration failed: %w", err)
	}

	// Run the daemon until the context is cancelled.
	d := daemon.New(daemon.Config{
		AgentID:         conf.AgentID,
		ControlPlaneURL: conf.ControlPlaneURL,
		PreSharedToken:  conf.PreSharedToken,
		Labels:          conf.Labels,
		Capabilities:    conf.Capabilities,
	})

	logger.Infoln("Agent daemon running. Press Ctrl+C to stop.")

	return d.Run(ctx)
}
