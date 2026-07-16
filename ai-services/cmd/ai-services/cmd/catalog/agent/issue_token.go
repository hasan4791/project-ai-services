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
		Long: `Generates a single-use 24-hour bootstrap token for the given agent ID and
prints the agent_id and pre_shared_token to paste into /etc/ai-services/agent.conf
on the Worker LPAR.

The control-plane API server must already be running with --agentgateway-port.`,
		Example: `  # Issue a token for lpar-1 (log in first)
  ai-services catalog agent issue-token lpar-1`,
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

			printAgentConf(os.Stdout, agentID, token)
			return nil
		},
	}

	return cmd
}

// printAgentConf writes the agent.conf template to w with agent_id and
// pre_shared_token filled in; all other fields are left as placeholders.
func printAgentConf(w *os.File, agentID, token string) {
	fmt.Fprintln(w, "# Copy the block below to /etc/ai-services/agent.conf on the Worker LPAR.")
	fmt.Fprintln(w, "# Token expires in 24 h and is single-use.")
	fmt.Fprintln(w, "# Then run:  ai-services agent start")
	fmt.Fprintln(w, "#")
	fmt.Fprintln(w, "# ---- BEGIN agent.conf ----")
	fmt.Fprintln(w, "control_plane_url: <control-plane-host>:<agentgateway-port>")
	fmt.Fprintf(w, "agent_id: %q\n", agentID)
	fmt.Fprintf(w, "pre_shared_token: %q\n", token)
	fmt.Fprintln(w, "labels: {}")
	fmt.Fprintln(w, "capabilities: {}")
	fmt.Fprintln(w, "# ---- END agent.conf ----")
}
