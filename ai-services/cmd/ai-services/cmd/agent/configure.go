package agent

import (
	"github.com/spf13/cobra"

	"github.com/project-ai-services/ai-services/internal/pkg/agent/configure"
	"github.com/project-ai-services/ai-services/internal/pkg/constants"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
)

func newConfigureCmd() *cobra.Command {
	var (
		baseDir     string
		httpsPort   int
		runtimeName string
	)

	cmd := &cobra.Command{
		Use:   "configure",
		Short: "Deploy the worker Caddy reverse proxy for external service traffic",
		Long: `Deploy the worker-side Caddy pod on this LPAR for external service traffic.

Caddy handles external HTTPS traffic for service endpoints deployed on this
worker. Routes are registered dynamically by the control-plane deployer over
the gRPC agent stream when applications are deployed.

Run this once before 'agent start' on each worker LPAR. No configuration
needed — all defaults are derived automatically.`,
		Example: `  ai-services agent configure --runtime podman
  ai-services agent configure --runtime podman --https-port 8443
  ai-services agent configure --runtime podman --basedir /custom/path`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			return configure.Run(configure.Options{
				BaseDir:     baseDir,
				HTTPSPort:   httpsPort,
				RuntimeType: types.RuntimeType(runtimeName),
			})
		},
	}

	cmd.Flags().StringVarP(&runtimeName, "runtime", "r", "podman", "Runtime to use (podman or openshift)")
	cmd.Flags().StringVar(&baseDir, "basedir", constants.DefaultBaseDir, "Base directory for Caddy data (optional)")
	cmd.Flags().IntVar(&httpsPort, "https-port", 443, "HTTPS port for external service traffic (optional)")

	return cmd
}
