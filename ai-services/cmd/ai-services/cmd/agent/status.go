package agent

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/project-ai-services/ai-services/internal/pkg/agent/agentconfig"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/client"
)

func newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show live connectivity status for this agent",
		Long: `Query the control plane for the live registry status of this agent.

The agent name is read from ~/.config/ai-services/agent.json,
which is written automatically by 'ai-services agent start'.

For a list of all registered agents run on the control plane:
  ai-services catalog agent list`,
		Example: `  ai-services agent status`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			cfg, err := agentconfig.Load()
			if err != nil {
				return err
			}

			return runStatus(cfg.AgentName)
		},
	}

	return cmd
}

func runStatus(agentName string) error {
	apiClient, err := client.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot connect to control plane API: %v\n", err)
		fmt.Fprintf(os.Stderr, "  Run `ai-services login` first if you have not authenticated.\n")
		return err
	}

	status, err := apiClient.GetAgent(agentName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: could not fetch status for agent %q: %v\n", agentName, err)
		fmt.Fprintf(os.Stderr, "  The agent may not have registered yet. Run: ai-services agent start\n")
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Live Status (from Control Plane)")
	fmt.Fprintln(w, "--------------------------------")
	fmt.Fprintf(w, "Agent Name:\t%s\n", status.AgentName)
	fmt.Fprintf(w, "Status:\t%s\n", status.Status)
	fmt.Fprintf(w, "Active slots:\t%d\n", status.ActiveSlots)
	if status.LastHeartbeat != "" {
		fmt.Fprintf(w, "Last heartbeat:\t%s\n", status.LastHeartbeat)
	}
	fmt.Fprintf(w, "Checked at:\t%s\n", time.Now().Format(time.RFC3339))
	w.Flush()

	return nil
}
