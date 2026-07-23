// Package agent provides the `ai-services agent` subcommand.
package agent

import "github.com/spf13/cobra"

// AgentCmd returns the cobra command for the agent daemon.
func AgentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Manage the AI Services worker agent daemon",
		Long: `The agent daemon runs on Worker LPARs and connects to the control-plane
	AgentGateway over a bidirectional gRPC CommandStream. It executes runtime
	commands on behalf of the control plane.
	
	Typical workflow on a Worker LPAR:
	
		 1. ai-services bootstrap configure --runtime podman
		    Install Podman, configure Spyre cards, ulimits, SELinux, SMT.
	
		 2. ai-services agent configure
		    Deploy the worker Caddy reverse proxy for external service traffic.
		    Saves the Caddy admin URL to local config for use by 'agent start'.
	
		 3. Obtain a bootstrap token (on the control plane):
		      ai-services catalog agent issue-token
	
		 4. ai-services agent start --server <host:port> --name <name> --token <token>
		    Register with the control plane and start the persistent CommandStream.
		    Caddy route registration is enabled automatically if step 2 was run.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(newConfigureCmd())
	cmd.AddCommand(newStartCmd())
	cmd.AddCommand(newStatusCmd())

	return cmd
}
