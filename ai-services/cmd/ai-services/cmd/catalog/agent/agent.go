// Package catalogagent provides the `ai-services catalog agent` subcommand
// for control-plane operators to manage worker agent registrations.
package catalogagent

import "github.com/spf13/cobra"

// NewCatalogAgentCmd returns the `catalog agent` parent command.
func NewCatalogAgentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Manage worker agent registrations on the control plane",
		Long: `Commands for control-plane operators to manage remote worker agents.

Use 'issue-token' to generate a bootstrap token before provisioning a new
Worker LPAR. Copy the returned token into /etc/ai-services/agent.conf on the
Worker LPAR, then run:

  ai-services agent start`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(newIssueTokenCmd())
	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newDeleteCmd())

	return cmd
}
