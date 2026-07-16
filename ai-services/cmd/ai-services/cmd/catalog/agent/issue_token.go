package catalogagent

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	catalogclient "github.com/project-ai-services/ai-services/internal/pkg/catalog/client"
)

func newIssueTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "issue-token <agent-id>",
		Short: "Issue a bootstrap token for a new Worker LPAR",
		Long: `Generates a single-use 24-hour bootstrap token for the given agent ID.

The token must be written into /etc/ai-services/agent.conf on the Worker LPAR
before running bootstrap configure. The control-plane API server must already
be running with --agentgateway-port.`,
		Example: `  # Issue a token for lpar-1 (you must be logged in first)
  ai-services catalog login --server https://control-plane:8080
  ai-services catalog agent issue-token lpar-1

  # The returned token goes into /etc/ai-services/agent.conf on the Worker:
  #   control_plane_url: "control-plane:9090"
  #   agent_id: "lpar-1"
  #   pre_shared_token: "<token>"`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			agentID := args[0]

			client, err := catalogclient.New()
			if err != nil {
				return fmt.Errorf("not logged in – run 'ai-services catalog login' first: %w", err)
			}

			token, err := client.IssueAgentToken(agentID)
			if err != nil {
				return fmt.Errorf("issue-token failed: %w", err)
			}

			fmt.Fprintf(os.Stdout, "Bootstrap token for agent '%s':\n\n  %s\n\n", agentID, token)
			fmt.Fprintln(os.Stdout, "Copy this token into /etc/ai-services/agent.conf on the Worker LPAR:")
			fmt.Fprintf(os.Stdout, "  pre_shared_token: \"%s\"\n\n", token)
			fmt.Fprintln(os.Stdout, "Token expires in 24 h and is single-use.")

			return nil
		},
	}

	return cmd
}
