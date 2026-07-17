package podman

import (
	"context"
	"fmt"

	"github.com/project-ai-services/ai-services/internal/pkg/catalog/cli/common/podman/deploy"
	catalogUtils "github.com/project-ai-services/ai-services/internal/pkg/catalog/utils"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	"github.com/project-ai-services/ai-services/internal/pkg/utils"
)

// ResetAgentGatewayPort redeploys the catalog pod with the given agentGatewayPort.
// Pass 0 to disable the AgentGateway entirely.
func ResetAgentGatewayPort(newPort int) error {
	deployCtx, err := deploy.NewDeployContext()
	if err != nil {
		return err
	}

	shouldProceed, err := validateCatalogServiceAndConfirmReset(deployCtx.Runtime, "agentgateway-port")
	if err != nil {
		return err
	}
	if !shouldProceed {
		return nil
	}

	opts, podID, err := catalogUtils.GetCatalogPodConfig(deployCtx.Runtime)
	if err != nil {
		return fmt.Errorf("failed to get existing catalog pod details: %w", err)
	}

	// Override the gateway port with the caller-supplied value.
	opts.AgentGatewayPort = newPort

	logger.InfofCtx(context.Background(), "Deleting existing catalog pod %s", podID)
	if err := deployCtx.Runtime.DeletePod(podID, utils.BoolPtr(true)); err != nil {
		return fmt.Errorf("failed to delete existing catalog pod: %w", err)
	}

	_, err = executeCatalogDeployment(context.Background(), deployCtx, *opts, "")
	if err != nil {
		return fmt.Errorf("failed to deploy catalog pod: %w", err)
	}

	return nil
}
