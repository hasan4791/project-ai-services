// Package configure implements `ai-services agent configure`.
// It deploys the worker-side Caddy pod and generates the Caddyfile.
// The Caddy admin URL is always derived at runtime by inspecting the running
// pod — it is never stored in config or asked from the user.
package configure

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/project-ai-services/ai-services/assets"
	clipodman "github.com/project-ai-services/ai-services/internal/pkg/cli/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/constants"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	podmodels "github.com/project-ai-services/ai-services/internal/pkg/models"
	"github.com/project-ai-services/ai-services/internal/pkg/proxy"
	podmanRuntime "github.com/project-ai-services/ai-services/internal/pkg/runtime/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
	"github.com/project-ai-services/ai-services/internal/pkg/specs"
	k8syaml "sigs.k8s.io/yaml"
)

const (
	// AgentCaddyPodName is the fixed name of the worker Caddy pod.
	// Exported so that `agent start` can inspect it to construct the admin URL.
	AgentCaddyPodName = "ai-services--agent-caddy"

	// caddyImageDefault is the default Caddy image, mirroring the catalog value.
	caddyImageDefault = "icr.io/ai-services-cicd/caddy:v2.11.4-0"

	dirPerm  = 0o755
	filePerm = 0o644
)

// Options holds the configuration for the agent configure command.
type Options struct {
	// BaseDir is the agent's data directory (mirrors catalog's BaseDir).
	BaseDir string
	// HTTPSPort is the port Caddy listens on for external HTTPS traffic.
	HTTPSPort int
	// RuntimeType is the local container runtime (podman or openshift).
	// Defaults to podman when empty.
	RuntimeType types.RuntimeType
}

// Run deploys the worker Caddy pod and generates the Caddyfile.
// The admin URL is constructed from the running pod at runtime — nothing is
// stored in config or asked from the user.
func Run(opts Options) error {
	ctx := context.Background()

	if opts.RuntimeType == "" {
		opts.RuntimeType = types.RuntimeTypePodman
	}

	// Only Podman is supported for the worker Caddy deployment.
	if opts.RuntimeType != types.RuntimeTypePodman {
		return fmt.Errorf("agent configure: runtime %q not supported — only podman is supported for worker Caddy deployment", opts.RuntimeType)
	}

	// Step 1: Generate Caddyfile on disk.
	if err := generateCaddyfile(opts.BaseDir); err != nil {
		return fmt.Errorf("agent configure: generate Caddyfile: %w", err)
	}
	logger.Infoln("Caddyfile generated.")

	// Step 2: Render and deploy the Caddy pod template.
	rt, err := podmanRuntime.NewPodmanClient()
	if err != nil {
		return fmt.Errorf("agent configure: init podman: %w", err)
	}

	// Check if already deployed.
	if exists, _ := rt.PodExists(AgentCaddyPodName); exists {
		logger.Infof("Worker Caddy pod '%s' already running — skipping deploy.\n", AgentCaddyPodName)
	} else {
		if err := deployCaddyPod(ctx, rt, opts); err != nil {
			return fmt.Errorf("agent configure: deploy Caddy pod: %w", err)
		}
		logger.Infof("Worker Caddy pod '%s' deployed.\n", AgentCaddyPodName)
	}

	// Step 3: Construct admin URL from the running pod and verify Caddy is healthy.
	adminURL, err := BuildAdminURL(rt)
	if err != nil {
		return fmt.Errorf("agent configure: resolve Caddy admin URL: %w", err)
	}

	pm := proxy.NewCaddyManager(adminURL, constants.CaddyServerName)
	if err := pm.HealthCheck(); err != nil {
		return fmt.Errorf("agent configure: Caddy health check failed: %w", err)
	}

	logger.Infof("\nWorker Caddy configured successfully.\n")
	logger.Infof("  Admin URL : %s\n", adminURL)
	logger.Infof("  HTTPS Port: %d\n", opts.HTTPSPort)
	logger.Infoln("\nRun 'ai-services agent start' to connect to the control plane.")

	return nil
}

// generateCaddyfile writes the Caddyfile template to BaseDir/common/caddy/Caddyfile.
func generateCaddyfile(baseDir string) error {
	raw, err := assets.AgentFS.ReadFile("agent/podman/Caddyfile.tmpl")
	if err != nil {
		return fmt.Errorf("read Caddyfile template: %w", err)
	}

	tmpl, err := template.New("Caddyfile").Parse(string(raw))
	if err != nil {
		return fmt.Errorf("parse Caddyfile template: %w", err)
	}

	data := map[string]any{
		"CaddyServerName": constants.CaddyServerName,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("render Caddyfile: %w", err)
	}

	caddyDir := filepath.Join(baseDir, "common", "caddy")
	if err := os.MkdirAll(caddyDir, dirPerm); err != nil {
		return fmt.Errorf("create caddy dir: %w", err)
	}

	return os.WriteFile(filepath.Join(caddyDir, "Caddyfile"), buf.Bytes(), filePerm)
}

// deployCaddyPod renders caddy.yaml.tmpl and deploys the pod.
func deployCaddyPod(ctx context.Context, rt *podmanRuntime.PodmanClient, opts Options) error {
	raw, err := assets.AgentFS.ReadFile("agent/podman/caddy.yaml.tmpl")
	if err != nil {
		return fmt.Errorf("read caddy pod template: %w", err)
	}

	tmpl, err := template.New("caddy-pod").Parse(string(raw))
	if err != nil {
		return fmt.Errorf("parse caddy pod template: %w", err)
	}

	data := map[string]any{
		"PodName":    AgentCaddyPodName,
		"BaseDir":    opts.BaseDir,
		"HTTPSPort":  opts.HTTPSPort,
		"CaddyImage": caddyImageDefault,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("render caddy pod template: %w", err)
	}

	// Parse the rendered YAML to get the PodSpec for readiness checks.
	var podSpec podmodels.PodSpec
	if err := k8syaml.Unmarshal(buf.Bytes(), &podSpec); err != nil {
		return fmt.Errorf("parse caddy pod spec: %w", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	podDeployOptions := clipodman.ConstructPodDeployOptions(specs.FetchPodAnnotations(podSpec))

	return clipodman.DeployPodAndReadinessCheck(ctx, rt, &podSpec, "caddy.yaml.tmpl", reader, podDeployOptions)
}

// BuildAdminURL returns the fixed Caddy admin URL for the worker pod.
// The admin port is always published as 127.0.0.1:2019:2019 — bound to the
// host loopback only, so it is unreachable from outside the Worker LPAR.
// No pod inspection needed: the URL is always http://localhost:2019.
func BuildAdminURL(_ *podmanRuntime.PodmanClient) (string, error) {
	return "http://localhost:2019", nil
}
