package catalogagent

import (
	"fmt"

	"github.com/spf13/cobra"

	catalogclient "github.com/project-ai-services/ai-services/internal/pkg/catalog/client"
)

func newDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <agent-name>",
		Short: "Delete a registered worker agent",
		Long: `Remove a worker agent from the control-plane AgentGateway registry.

This deletes the agent record from both the in-memory registry and the
PostgreSQL database. If the agent currently has an active CommandStream open,
it will receive an error on its next send/recv and disconnect.

The agent can re-register by running 'ai-services agent start' with a newly
issued token.`,
		Example: `  ai-services catalog agent delete lpar-1
  ai-services catalog agent delete worker-us-east-01`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			agentName := args[0]

			client, err := catalogclient.New()
			if err != nil {
				return fmt.Errorf("not logged in – run 'ai-services catalog login' first: %w", err)
			}

			if err := client.DeleteAgent(agentName); err != nil {
				return fmt.Errorf("delete agent failed: %w", err)
			}

			fmt.Printf("Agent %q deleted.\n", agentName)
			return nil
		},
	}

	return cmd
}
