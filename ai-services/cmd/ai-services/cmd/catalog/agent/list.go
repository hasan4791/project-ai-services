package catalogagent

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	catalogclient "github.com/project-ai-services/ai-services/internal/pkg/catalog/client"
)

func newListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all registered worker agents",
		Long:  `Show the status of all worker agents registered with the control-plane AgentGateway.`,
		Example: `  ai-services catalog agent list`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			client, err := catalogclient.New()
			if err != nil {
				return fmt.Errorf("not logged in – run 'ai-services catalog login' first: %w", err)
			}

			agents, err := client.ListAgents()
			if err != nil {
				return fmt.Errorf("list agents failed: %w", err)
			}

			if len(agents) == 0 {
				fmt.Fprintln(os.Stdout, "No agents registered.")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "AGENT NAME\tSTATUS\tLAST HEARTBEAT\tLABELS")
			for _, a := range agents {
				agentName, _ := a["agent_name"].(string)
				status, _ := a["status"].(string)
				hb, _ := a["last_heartbeat"].(string)
				if hb == "" {
					hb = "—"
				}
				labels := ""
				if lm, ok := a["labels"].(map[string]any); ok {
					for k, v := range lm {
						if labels != "" {
							labels += ","
						}
						labels += fmt.Sprintf("%s=%v", k, v)
					}
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", agentName, status, hb, labels)
			}
			w.Flush()

			return nil
		},
	}

	return cmd
}
