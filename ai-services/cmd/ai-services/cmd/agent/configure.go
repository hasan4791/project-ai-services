package agent

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	agentconfigure "github.com/project-ai-services/ai-services/internal/pkg/agent/configure"
	"github.com/project-ai-services/ai-services/internal/pkg/constants"
	"github.com/project-ai-services/ai-services/internal/pkg/utils"
)

func newConfigureCmd() *cobra.Command {
	var (
		baseDir     string
		runtimeName string
		httpsPort   int
	)

	cmd := &cobra.Command{
		Use:   "configure",
		Short: "Set up the Worker-side Caddy proxy pod (run once before agent start)",
		Long: `Deploy the agent Caddy pod on this Worker LPAR.

The Worker Caddy listens on hostPort 8443 so the control-plane Caddy can
reverse-proxy to it directly.  Its admin API is bound to localhost:2019
so only the agent daemon can register and remove routes dynamically.

This command is idempotent — re-running it will remove any existing pod
and redeploy fresh to ensure the correct port bindings are in place.

Run this once after 'ai-services bootstrap configure', before 'agent start'.`,
		Example: `  ai-services agent configure --runtime podman
		ai-services agent configure --runtime podman --base-dir /custom/path
		ai-services agent configure --runtime podman --https-port 8443`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			var (
				resolvedDir string
				err         error
			)
			if baseDir == "" {
				resolvedDir = constants.DefaultBaseDir
			} else {
				resolvedDir, err = utils.ValidateBaseDir(baseDir)
				if err != nil {
					return fmt.Errorf("invalid base directory %q: %w", baseDir, err)
				}
			}

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			return agentconfigure.DeployAgentCaddy(ctx, agentconfigure.Options{
				BaseDir:   resolvedDir,
				Runtime:   runtimeName,
				HTTPSPort: httpsPort,
			})
		},
	}

	cmd.Flags().StringVarP(&runtimeName, "runtime", "r", "podman", "Local container runtime (podman or openshift)")
	cmd.Flags().StringVar(&baseDir, "base-dir", "", fmt.Sprintf("Root data directory on this Worker LPAR (default: %s)", constants.DefaultBaseDir))
	cmd.Flags().IntVar(&httpsPort, "https-port", 443, "Host port Caddy listens on for external HTTPS traffic")

	return cmd
}
