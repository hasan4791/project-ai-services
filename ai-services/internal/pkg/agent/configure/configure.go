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
	clipodman "github.com/project-ai-services/ai-services/internal/pkg/cli/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/cli/common/podman/caddy"
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
	// Defaults to 8443.
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

	// Resolve Caddy image from values.yaml.
	caddyImage, err := defaultCaddyImage()
	if err != nil {
		return fmt.Errorf("agent configure: resolve caddy image: %w", err)
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
	httpsPort := opts.HTTPSPort
	if httpsPort == 0 {
		httpsPort = 8443
	}
	if err := deployAgentCaddyPod(ctx, rt, opts.BaseDir, caddyImage, httpsPort); err != nil {
		return err
	}

	// Step 4 — verify Caddy admin API is reachable.
	adminURL, _ := BuildAdminURL()
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

	logger.InfofCtx(ctx, "agent configure: Worker Caddy ready — admin %s, HTTPS :%d, domain *.%s\n", adminURL, httpsPort, domainSuffix)
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
func deployAgentCaddyPod(ctx context.Context, rt *podman.PodmanClient, baseDir, caddyImage string, httpsPort int) error {
	raw, err := assets.AgentFS.ReadFile(agentCaddyTmplName)
	if err != nil {
		return fmt.Errorf("agent configure: read pod template: %w", err)
	}

	tmpl, err := template.New("agent-caddy.yaml.tmpl").Parse(string(raw))
	if err != nil {
		return fmt.Errorf("agent configure: parse pod template: %w", err)
	}

	params := map[string]any{
		"BaseDir":   baseDir,
		"HTTPSPort": httpsPort,
		"Values": map[string]any{
			"caddy": map[string]any{
				"image": caddyImage,
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

	logger.InfofCtx(ctx, "agent configure: %s is ready — Worker Caddy listening on :8443\n", AgentCaddyPodName)
	return nil
}

// defaultCaddyImage reads the Caddy image default from assets/agent/podman/values.yaml.
func defaultCaddyImage() (string, error) {
	raw, err := assets.AgentFS.ReadFile("agent/podman/values.yaml")
	if err != nil {
		return "", fmt.Errorf("read agent values.yaml: %w", err)
	}

	var vals struct {
		Caddy struct {
			Image string `yaml:"image"`
		} `yaml:"caddy"`
	}
	if err := k8syaml.Unmarshal(raw, &vals); err != nil {
		return "", fmt.Errorf("parse agent values.yaml: %w", err)
	}
	if vals.Caddy.Image == "" {
		return "", fmt.Errorf("caddy.image not set in agent/podman/values.yaml")
	}
	return vals.Caddy.Image, nil
}

// BuildAdminURL returns the fixed Caddy admin URL for the worker pod.
// The admin port is always published as 127.0.0.1:2019:2019 — bound to the
// host loopback only, so it is unreachable from outside the Worker LPAR.
// No pod inspection needed: the URL is always http://localhost:2019.
func BuildAdminURL() (string, error) {
	return "http://localhost:2019", nil
}
