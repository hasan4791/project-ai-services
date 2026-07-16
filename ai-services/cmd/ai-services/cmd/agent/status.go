package agent

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/project-ai-services/ai-services/internal/pkg/agent/agentbootstrap"
)

func newStatusCmd() *cobra.Command {
	var confPath = agentbootstrap.DefaultAgentConfPath

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show agent configuration and connectivity status",
		Long: `Show the current configuration of this worker agent and verify that the
agent.conf file is present and readable.

For live connectivity status of all registered agents, run on the control plane:
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
	fmt.Fprintln(w, "Agent Configuration")
	fmt.Fprintln(w, "-------------------")
	fmt.Fprintf(w, "Agent ID:\t%s\n", conf.AgentID)
	fmt.Fprintf(w, "Control Plane:\t%s\n", conf.ControlPlaneURL)
	fmt.Fprintf(w, "Config file:\t%s\n", confPath)
	fmt.Fprintf(w, "Token set:\t%v\n", conf.PreSharedToken != "")
	if len(conf.Labels) > 0 {
		fmt.Fprintf(w, "Labels:\n")
		for k, v := range conf.Labels {
			fmt.Fprintf(w, "  %s:\t%s\n", k, v)
		}
	}
	fmt.Fprintf(w, "Checked at:\t%s\n", time.Now().Format(time.RFC3339))
	w.Flush()

	return nil
}
