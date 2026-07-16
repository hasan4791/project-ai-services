package deployment

import (
	"context"
	"fmt"

	"github.com/project-ai-services/ai-services/internal/pkg/catalog"
	apimodels "github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver/models"
	"github.com/project-ai-services/ai-services/internal/pkg/vars"

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
}

// NewDeploymentExecutor creates a new DeploymentExecutor instance.
func NewDeploymentExecutor(
	catalogProvider *catalog.CatalogProvider,
	appRepo repository.ApplicationRepository,
	serviceRepo repository.ServiceRepository,
	componentRepo repository.ComponentRepository,
) *DeploymentExecutor {
	return &DeploymentExecutor{
		planner:         NewDeploymentPlanner(catalogProvider, componentRepo),
		catalogProvider: catalogProvider,
		appRepo:         appRepo,
		serviceRepo:     serviceRepo,
		componentRepo:   componentRepo,
	}
}

// ExecuteWithPlan executes deployment using an existing plan.
// This is used when the plan has already been created and database records inserted.
func (e *DeploymentExecutor) ExecuteWithPlan(
	ctx context.Context,
	plan *DeploymentPlan,
	req apimodels.CreateApplicationRequest,
	runtimeType types.RuntimeType,
) error {
	if err := e.executeDeployment(ctx, plan, req, runtimeType); err != nil {
		return fmt.Errorf("failed to execute deployment: %w", err)
	}
	return nil
}

// executeDeployment executes the deployment plan using the appropriate runtime deployer.
func (e *DeploymentExecutor) executeDeployment(
	ctx context.Context,
	plan *DeploymentPlan,
	req apimodels.CreateApplicationRequest,
	runtimeType types.RuntimeType,
) error {
	switch runtimeType {
	case types.RuntimeTypePodman:
		return e.executePodmanDeployment(ctx, plan, req)
	case types.RuntimeTypeRemote:
		return e.executeRemoteDeployment(ctx, plan, req)
	case types.RuntimeTypeOpenShift:
		return fmt.Errorf("OpenShift deployment not yet implemented")
	default:
		return fmt.Errorf("unsupported runtime type: %s", runtimeType)
	}
}

// executePodmanDeployment executes deployment for the local Podman runtime.
func (e *DeploymentExecutor) executePodmanDeployment(
	ctx context.Context,
	plan *DeploymentPlan,
	req apimodels.CreateApplicationRequest,
) error {
	rt, err := podmanRuntime.NewPodmanClient()
	if err != nil {
		return fmt.Errorf("failed to initialize Podman runtime: %w", err)
	}
	return e.runDeployer(ctx, rt, plan, req)
}

// executeRemoteDeployment executes deployment via a remote worker agent.
// It reads the agent selector from the plan's AgentSelector field (if set)
// and delegates to the RuntimeFactory which must already hold a RemoteRuntime.
func (e *DeploymentExecutor) executeRemoteDeployment(
	ctx context.Context,
	plan *DeploymentPlan,
	req apimodels.CreateApplicationRequest,
) error {
	// The caller (ApplicationService) is responsible for providing the correct
	// runtime via vars.RuntimeFactory when runtime=remote.  We retrieve it here.
	rt, err := getRuntimeFromFactory()
	if err != nil {
		return fmt.Errorf("failed to get remote runtime: %w", err)
	}
	return e.runDeployer(ctx, rt, plan, req)
}

// runDeployer creates a PodmanDeployer with the provided runtime and executes it.
func (e *DeploymentExecutor) runDeployer(
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

// getRuntimeFromFactory obtains a runtime from vars.RuntimeFactory.
// For RuntimeTypeRemote this returns an error because RemoteRuntime instances
// are always created by AgentDispatcher.SelectAgent; the caller should have
// set a concrete runtime on the plan before invoking executeRemoteDeployment.
func getRuntimeFromFactory() (runtime.Runtime, error) {
	if vars.RuntimeFactory == nil {
		return nil, fmt.Errorf("RuntimeFactory not initialised")
	}
	return vars.RuntimeFactory.Create("")
}
