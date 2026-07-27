// Package configure implements the one-time Worker-side setup that deploys the
// agent Caddy pod.  It mirrors the pattern used by catalog configure:
//
//  1. Write the base Caddyfile to <baseDir>/agent/caddy/Caddyfile
//  2. Render agent-caddy.yaml.tmpl with the Caddy image and base dir
//  3. Deploy (or skip if already running) via clipodman.DeployPodAndReadinessCheck
//
// This is intentionally separate from `agent start` so that the daemon loop
// can be restarted freely without re-deploying infrastructure.
package configure

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/project-ai-services/ai-services/assets"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/agentconfig"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/cli/common/podman/caddy"
	clipodman "github.com/project-ai-services/ai-services/internal/pkg/cli/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/constants"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	podmodels "github.com/project-ai-services/ai-services/internal/pkg/models"
	"github.com/project-ai-services/ai-services/internal/pkg/proxy"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/specs"
	k8syaml "sigs.k8s.io/yaml"
)

const (
	// AgentCaddyPodName is the fixed name of the worker Caddy pod.
	// Exported so that `agent start` can use it.
	AgentCaddyPodName = "ai-services--agent-caddy"

	agentCaddyfileSubDir = "agent/caddy"
	agentCaddyTmplName   = "agent/podman/templates/agent-caddy.yaml.tmpl"
	agentCaddyfileTmpl   = "agent/podman/templates/agent-caddyfile.tmpl"

	dirPerm  = 0o755
	filePerm = 0o644
)

// Options holds the parameters for agent configure.
type Options struct {
	// BaseDir is the root data directory on the Worker, e.g. /var/lib/ai-services.
	// Caddy data is written to <BaseDir>/agent/caddy/.
	BaseDir string
	// Runtime is the local container runtime ("podman" or "openshift").
	// Defaults to "podman". Only podman is supported for worker Caddy deployment.
	Runtime string
	// HTTPSPort is the host port Caddy listens on for external HTTPS traffic.
	// Defaults to 443.
	HTTPSPort int
	// DomainName is an optional custom domain (e.g. "example.com").
	// Priority: SSLCertPath > DomainName > workerIP.nip.io (auto-detected).
	DomainName string
	// SSLCertPath and SSLKeyPath are optional paths to a wildcard TLS cert/key.
	// When provided the domain is extracted from the certificate.
	SSLCertPath string
	SSLKeyPath  string
}

// DeployAgentCaddy writes the Caddyfile, deploys the agent Caddy pod, and
// persists the computed domain suffix to agentconfig for use by 'agent start'.
func DeployAgentCaddy(ctx context.Context, opts Options) error {
	if opts.Runtime == "" {
		opts.Runtime = "podman"
	}
	if opts.Runtime != "podman" {
		return fmt.Errorf("agent configure: runtime %q not supported — only podman is supported for worker Caddy deployment", opts.Runtime)
	}

	rt, err := podman.NewPodmanClient()
	if err != nil {
		return fmt.Errorf("agent configure: init podman client: %w", err)
	}

	// Read all values from values.yaml — single source of truth for image,
	// adminPort and httpsPort, matching the catalog pattern.
	vals, err := readAgentValues()
	if err != nil {
		return fmt.Errorf("agent configure: read values: %w", err)
	}
	if vals.Caddy.Image == "" {
		return fmt.Errorf("agent configure: caddy.image not set in values.yaml")
	}
	if vals.Caddy.AdminPort == "" {
		vals.Caddy.AdminPort = "0"
	}
	// CLI flag overrides values.yaml; fall back to values.yaml default (443).
	if opts.HTTPSPort > 0 {
		vals.Caddy.HTTPSPort = opts.HTTPSPort
	} else if vals.Caddy.HTTPSPort == 0 {
		vals.Caddy.HTTPSPort = 443
	}

	// Step 1 — write the Caddyfile to disk so the pod volume-mount picks it up.
	if err := writeAgentCaddyfile(opts.BaseDir); err != nil {
		return err
	}

	// Step 2 — remove any existing pod so we always deploy fresh with the
	// correct 127.0.0.1:2019:2019 binding (handles stale/failed pods too).
	exists, err := rt.PodExists(AgentCaddyPodName)
	if err != nil {
		return fmt.Errorf("agent configure: check pod existence: %w", err)
	}
	if exists {
		logger.InfofCtx(ctx, "agent configure: %s exists — removing before redeploy\n", AgentCaddyPodName)
		force := true
		if err := rt.DeletePod(AgentCaddyPodName, &force); err != nil {
			return fmt.Errorf("agent configure: remove existing pod: %w", err)
		}
	}

	// Step 3 — render pod template and deploy.
	if err := deployAgentCaddyPod(ctx, rt, opts.BaseDir, vals); err != nil {
		return err
	}

	// Step 4 — resolve admin URL by inspecting the running pod (random host port).
	adminURL, err := BuildAdminURL(rt)
	if err != nil {
		return fmt.Errorf("agent configure: resolve admin URL: %w", err)
	}
	pm := proxy.NewCaddyManager(adminURL, constants.AgentCaddyServerName)
	if err := pm.HealthCheck(); err != nil {
		return fmt.Errorf("agent configure: Caddy health check failed: %w", err)
	}

	// Step 5 — compute domain suffix (same priority as catalog configure) and
	// persist it so 'agent start' can send it to the control plane.
	domainSuffix, err := caddy.ComputeDomainConfig(opts.SSLCertPath, opts.SSLKeyPath, opts.DomainName)
	if err != nil {
		return fmt.Errorf("agent configure: compute domain suffix: %w", err)
	}

	if err := agentconfig.Save(agentconfig.AgentConfig{DomainSuffix: domainSuffix}); err != nil {
		logger.Warningf("agent configure: could not persist domain suffix: %v\n", err)
	}

	logger.InfofCtx(ctx, "agent configure: Worker Caddy ready — admin %s, HTTPS :%d, domain *.%s\n", adminURL, vals.Caddy.HTTPSPort, domainSuffix)
	logger.Infoln("Run 'ai-services agent start' to connect to the control plane.")
	return nil
}

// writeAgentCaddyfile renders the static Caddyfile template and writes it to
// <baseDir>/agent/caddy/Caddyfile, creating directories as needed.
func writeAgentCaddyfile(baseDir string) error {
	raw, err := assets.AgentFS.ReadFile(agentCaddyfileTmpl)
	if err != nil {
		return fmt.Errorf("agent configure: read caddyfile template: %w", err)
	}

	// The agent-caddyfile.tmpl has no template variables — write it verbatim.
	caddyDir := filepath.Join(baseDir, agentCaddyfileSubDir)
	if err := os.MkdirAll(caddyDir, dirPerm); err != nil {
		return fmt.Errorf("agent configure: create caddy dir %s: %w", caddyDir, err)
	}

	caddyfilePath := filepath.Join(caddyDir, "Caddyfile")
	if err := os.WriteFile(caddyfilePath, raw, filePerm); err != nil {
		return fmt.Errorf("agent configure: write Caddyfile to %s: %w", caddyfilePath, err)
	}

	logger.Infof("agent configure: Caddyfile written to %s\n", caddyfilePath)
	return nil
}

// deployAgentCaddyPod renders the pod template and calls DeployPodAndReadinessCheck.
func deployAgentCaddyPod(ctx context.Context, rt *podman.PodmanClient, baseDir string, vals *agentCaddyValues) error {
	raw, err := assets.AgentFS.ReadFile(agentCaddyTmplName)
	if err != nil {
		return fmt.Errorf("agent configure: read pod template: %w", err)
	}

	tmpl, err := template.New("agent-caddy.yaml.tmpl").Parse(string(raw))
	if err != nil {
		return fmt.Errorf("agent configure: parse pod template: %w", err)
	}

	// Mirror catalog template params: BaseDir + Values (caddy sub-map).
	params := map[string]any{
		"BaseDir": baseDir,
		"Values": map[string]any{
			"caddy": map[string]any{
				"image":     vals.Caddy.Image,
				"adminPort": vals.Caddy.AdminPort,
				"httpsPort": vals.Caddy.HTTPSPort,
			},
		},
	}

	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, params); err != nil {
		return fmt.Errorf("agent configure: render pod template: %w", err)
	}

	// Unmarshal the rendered YAML into PodSpec for readiness checks and deploy opts.
	var podSpec podmodels.PodSpec
	if err := k8syaml.Unmarshal(rendered.Bytes(), &podSpec); err != nil {
		return fmt.Errorf("agent configure: parse rendered pod yaml: %w", err)
	}

	deployOpts := clipodman.ConstructPodDeployOptions(specs.FetchPodAnnotations(podSpec))

	logger.InfofCtx(ctx, "agent configure: deploying %s\n", AgentCaddyPodName)
	if err := clipodman.DeployPodAndReadinessCheck(ctx, rt, &podSpec, "agent-caddy.yaml.tmpl",
		bytes.NewReader(rendered.Bytes()), deployOpts); err != nil {
		return fmt.Errorf("agent configure: deploy caddy pod: %w", err)
	}

	return nil
}

// agentCaddyValues holds the fields read from assets/agent/podman/values.yaml.
type agentCaddyValues struct {
	Caddy struct {
		Image     string `yaml:"image"`
		AdminPort string `yaml:"adminPort"`
		HTTPSPort int    `yaml:"httpsPort"`
	} `yaml:"caddy"`
}

// readAgentValues parses assets/agent/podman/values.yaml.
func readAgentValues() (*agentCaddyValues, error) {
	raw, err := assets.AgentFS.ReadFile("agent/podman/values.yaml")
	if err != nil {
		return nil, fmt.Errorf("read agent values.yaml: %w", err)
	}
	var vals agentCaddyValues
	if err := k8syaml.Unmarshal(raw, &vals); err != nil {
		return nil, fmt.Errorf("parse agent values.yaml: %w", err)
	}
	return &vals, nil
}

// BuildAdminURL inspects the running worker Caddy pod and returns the host-side
// admin URL (e.g. "http://localhost:37249").
func BuildAdminURL(rt *podman.PodmanClient) (string, error) {
	adminPort, err := proxy.GetCaddyAdminPort(rt, AgentCaddyPodName)
	if err != nil {
		return "", fmt.Errorf("resolve agent caddy admin port: %w", err)
	}
	return fmt.Sprintf("http://localhost:%s", adminPort), nil
}
