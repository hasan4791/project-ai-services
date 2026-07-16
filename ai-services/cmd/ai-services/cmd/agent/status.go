package agent

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/project-ai-services/ai-services/internal/pkg/agent/agentbootstrap"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/client"
)

func newStatusCmd() *cobra.Command {
	var confPath = agentbootstrap.DefaultAgentConfPath

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show agent configuration and live connectivity status",
		Long: `Show the local agent configuration and query the control plane for live
connectivity status of this agent.

Requires a valid agent.conf and that the control plane API is reachable.
For a list of all registered agents run on the control plane:
  ai-services catalog agent list`,
		Example: `  ai-services agent status
  ai-services agent status --conf /etc/ai-services/agent.conf`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			return runStatus(confPath)
		},
	}

	cmd.Flags().StringVar(&confPath, "conf", confPath, "Path to the agent configuration file")

	return cmd
}

func runStatus(confPath string) error {
	conf, err := agentbootstrap.LoadConf(confPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n\n", err)
		fmt.Fprintf(os.Stderr, "Agent is NOT configured. Run the following steps:\n")
		fmt.Fprintf(os.Stderr, "  1. ai-services bootstrap configure --runtime podman\n")
		fmt.Fprintf(os.Stderr, "  2. Write %s\n", confPath)
		fmt.Fprintf(os.Stderr, "     (control_plane_url, agent_id, pre_shared_token)\n")
		fmt.Fprintf(os.Stderr, "     Token: ai-services catalog agent issue-token <agent-id>\n")
		fmt.Fprintf(os.Stderr, "  3. ai-services agent start\n")
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Local Configuration")
	fmt.Fprintln(w, "-------------------")
	fmt.Fprintf(w, "Agent ID:\t%s\n", conf.AgentID)
	fmt.Fprintf(w, "Control Plane:\t%s\n", conf.ControlPlaneURL)
	fmt.Fprintf(w, "Config file:\t%s\n", confPath)
	fmt.Fprintf(w, "Token set:\t%v\n", conf.PreSharedToken != "")
	if len(conf.Labels) > 0 {
		fmt.Fprintln(w, "Labels:")
		for k, v := range conf.Labels {
			fmt.Fprintf(w, "  %s:\t%s\n", k, v)
		}
	}
	fmt.Fprintln(w)
	w.Flush()

	// Query the control plane for live status.
	apiClient, err := client.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: cannot connect to control plane API (%v)\n", err)
		fmt.Fprintf(os.Stderr, "  Run `ai-services login` first if you have not authenticated.\n")
		fmt.Fprintf(os.Stderr, "  Checked at: %s\n", time.Now().Format(time.RFC3339))
		return nil //nolint:nilerr // local conf is valid; live status is best-effort
	}

	status, err := apiClient.GetAgent(conf.AgentID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: could not fetch live status from control plane: %v\n", err)
		fmt.Fprintf(os.Stderr, "  The agent may not have registered yet. Run: ai-services agent start\n")
		fmt.Fprintf(os.Stderr, "  Checked at: %s\n", time.Now().Format(time.RFC3339))
		return nil //nolint:nilerr // not a fatal error
	}

	w2 := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w2, "Live Status (from Control Plane)")
	fmt.Fprintln(w2, "--------------------------------")
	fmt.Fprintf(w2, "Status:\t%s\n", status.Status)
	fmt.Fprintf(w2, "Active slots:\t%d\n", status.ActiveSlots)
	if status.LastHeartbeat != "" {
		fmt.Fprintf(w2, "Last heartbeat:\t%s\n", status.LastHeartbeat)
	}
	fmt.Fprintf(w2, "Checked at:\t%s\n", time.Now().Format(time.RFC3339))
	w2.Flush()

	return nil
}
