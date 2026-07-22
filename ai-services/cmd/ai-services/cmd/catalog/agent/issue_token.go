package catalogagent

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	catalogclient "github.com/project-ai-services/ai-services/internal/pkg/catalog/client"
)

func newIssueTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "issue-token",
		Short: "Issue a bootstrap token for a new Worker LPAR",
		Long: `Generates a single-use 24-hour bootstrap token and prints it.

Pass this token to the agent when starting it:
  ai-services agent start --server <host:port> --name <name> --token <token>

The control-plane API server must already be running with --agentgateway-port.`,
		Example: `  # Issue a token (log in first)
  ai-services catalog agent issue-token`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			client, err := catalogclient.New()
			if err != nil {
				return fmt.Errorf("not logged in – run 'ai-services catalog login' first: %w", err)
			}

			token, err := client.IssueAgentToken()
			if err != nil {
				return fmt.Errorf("issue-token failed: %w", err)
			}

			printToken(os.Stdout, token)
			return nil
		},
	}

	return cmd
}

// printToken writes the bootstrap token and usage hint to w.
func printToken(w *os.File, token string) {
	fmt.Fprintln(w, "Bootstrap token issued. It expires in 24 h and is single-use.")
	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "Token: %s\n", token)
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "On the Worker LPAR, run:")
	fmt.Fprintln(w, "  ai-services agent start \\")
	fmt.Fprintln(w, "    --server <control-plane-host>:<agentgateway-port> \\")
	fmt.Fprintln(w, "    --name   <your-agent-name> \\")
	fmt.Fprintf(w, "    --token  %s\n", token)
}
