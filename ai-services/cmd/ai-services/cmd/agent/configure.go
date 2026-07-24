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
		domainName  string
		sslCertPath string
		sslKeyPath  string
	)

	const defaultHTTPSPort = 443

	cmd := &cobra.Command{
		Use:   "configure",
		Short: "Set up the Worker-side Caddy proxy pod (run once before agent start)",
		Long: `Deploy the agent Caddy pod on this Worker LPAR.

The Worker Caddy listens on hostPort 443 (default) for external HTTPS traffic.
Its admin API is bound to a random loopback port assigned by the OS at runtime.

This command is idempotent — re-running it will remove any existing pod
and redeploy fresh to ensure the correct port bindings are in place.

Run this once after 'ai-services bootstrap configure', before 'agent start'.`,
		Example: `  ai-services agent configure --runtime podman
		ai-services agent configure --runtime podman --base-dir /custom/path
		ai-services agent configure --runtime podman --https-port 8443`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			if runtimeName == "" {
				return fmt.Errorf("--runtime is required (podman or openshift)")
			}

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
				BaseDir:     resolvedDir,
				Runtime:     runtimeName,
				HTTPSPort:   httpsPort,
				DomainName:  domainName,
				SSLCertPath: sslCertPath,
				SSLKeyPath:  sslKeyPath,
			})
		},
	}

	cmd.Flags().StringVarP(&runtimeName, "runtime", "r", "", "Local container runtime: podman or openshift (required)")
	cmd.Flags().StringVar(&baseDir, "base-dir", "", fmt.Sprintf("Root data directory on this Worker LPAR (default: %s)", constants.DefaultBaseDir))
	cmd.Flags().IntVar(&httpsPort, "https-port", 443, "Host port Caddy listens on for external HTTPS traffic")
	cmd.Flags().StringVar(&domainName, "domain-name", "", "Custom domain name for service routes (e.g. example.com). Defaults to <worker-ip>.nip.io")
	cmd.Flags().StringVar(&sslCertPath, "ssl-cert", "", "Path to wildcard SSL certificate (must be used with --ssl-key)")
	cmd.Flags().StringVar(&sslKeyPath, "ssl-key", "", "Path to SSL private key (must be used with --ssl-cert)")

	return cmd
}
