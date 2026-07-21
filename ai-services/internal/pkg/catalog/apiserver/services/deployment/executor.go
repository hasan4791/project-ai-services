package deployment

import (
	"context"
	"fmt"

	"github.com/project-ai-services/ai-services/internal/pkg/agent/dispatcher"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog"
	apimodels "github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver/models"

	"github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver/services/deployment/repository/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/db/repository"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime"
	podmanRuntime "github.com/project-ai-services/ai-services/internal/pkg/runtime/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
)

// DeploymentExecutor orchestrates the complete deployment process.
// It uses the DeploymentPlanner to create a plan and then executes it
// using the appropriate runtime-specific deployer.
type DeploymentExecutor struct {
	planner         *DeploymentPlanner
	catalogProvider *catalog.CatalogProvider
	appRepo         repository.ApplicationRepository
	serviceRepo     repository.ServiceRepository
	componentRepo   repository.ComponentRepository
	// dispatcher is optional; non-nil only when the AgentGateway is enabled.
	dispatcher *dispatcher.AgentDispatcher
}

// NewDeploymentExecutor creates a new DeploymentExecutor instance.
// dispatcher may be nil when the AgentGateway is not configured; in that case
// requests with an agent_selector will return an error at execution time.
func NewDeploymentExecutor(
	catalogProvider *catalog.CatalogProvider,
	appRepo repository.ApplicationRepository,
	serviceRepo repository.ServiceRepository,
	componentRepo repository.ComponentRepository,
	dispatcher *dispatcher.AgentDispatcher,
) *DeploymentExecutor {
	return &DeploymentExecutor{
		planner:         NewDeploymentPlanner(catalogProvider, componentRepo),
		catalogProvider: catalogProvider,
		appRepo:         appRepo,
		serviceRepo:     serviceRepo,
		componentRepo:   componentRepo,
		dispatcher:      dispatcher,
	}
}

// ExecuteWithPlan executes deployment using an existing plan.
// It resolves the correct runtime from the plan (local Podman or a remote agent),
// then asks the runtime itself what type it is to select the right deployer.
// This is used when the plan has already been created and database records inserted.
func (e *DeploymentExecutor) ExecuteWithPlan(
	ctx context.Context,
	plan *DeploymentPlan,
	req apimodels.CreateApplicationRequest,
) error {
	rt, err := e.resolveRuntime(plan)
	if err != nil {
		return err
	}

	switch rt.Type() {
	case types.RuntimeTypePodman, types.RuntimeTypeRemotePodman:
		// Local Podman and remote Podman agents both use PodmanDeployer.
		// RemoteRuntime proxies every call over gRPC to the worker's Podman socket,
		// so the deployer logic is identical.
		return e.runPodmanDeployer(ctx, rt, plan, req)
	case types.RuntimeTypeOpenShift, types.RuntimeTypeRemoteOpenShift:
		return fmt.Errorf("OpenShift deployment not yet implemented")
	default:
		return fmt.Errorf("unsupported runtime type: %s", rt.Type())
	}
}

// resolveRuntime returns the correct runtime for the plan.
// When plan.AgentSelector is set it picks a READY remote agent via the dispatcher.
// Otherwise it creates a local PodmanClient.
func (e *DeploymentExecutor) resolveRuntime(plan *DeploymentPlan) (runtime.Runtime, error) {
	if len(plan.AgentSelector) > 0 {
		if e.dispatcher == nil {
			return nil, fmt.Errorf("agent_selector provided but AgentGateway is not enabled on this server")
		}
		rt, _, err := e.dispatcher.SelectAgent(plan.AgentSelector)
		if err != nil {
			return nil, fmt.Errorf("no available worker agent: %w", err)
		}
		return rt, nil
	}

	rt, err := podmanRuntime.NewPodmanClient()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Podman runtime: %w", err)
	}
	return rt, nil
}

// runPodmanDeployer creates a PodmanDeployer with the provided runtime and executes it.
func (e *DeploymentExecutor) runPodmanDeployer(
	ctx context.Context,
	rt runtime.Runtime,
	plan *DeploymentPlan,
	req apimodels.CreateApplicationRequest,
) error {
	deployer := podman.NewPodmanDeployer(
		rt,
		e.catalogProvider,
		e.appRepo,
		e.serviceRepo,
		e.componentRepo,
	)
	return deployer.ExecuteDeployment(ctx, plan, req)
}

// Made with Bob
