// Package remote provides a runtime.Runtime implementation that dispatches
// every operation to a remote worker agent via the gRPC CommandStream.
// The RemoteRuntime is created per-request by the AgentDispatcher after an
// agent has been selected.
package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	agentpb "github.com/project-ai-services/ai-services/internal/pkg/agent/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/registry"
	"github.com/project-ai-services/ai-services/internal/pkg/models"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
)

const commandTimeout = 5 * time.Minute

// RemoteRuntime implements runtime.Runtime by sending commands over gRPC to a
// specific remote agent.
type RemoteRuntime struct {
	agentName string
	registry  *registry.Registry
}

// New creates a RemoteRuntime targeting the agent identified by agentName.
func New(agentName string, reg *registry.Registry) *RemoteRuntime {
	return &RemoteRuntime{agentName: agentName, registry: reg}
}

// ──────────────────────────────────────────────────────────────────────────────
// dispatch sends a Command and waits for the matching CommandResult.
// ──────────────────────────────────────────────────────────────────────────────

func (r *RemoteRuntime) dispatch(ctx context.Context, cmdType agentpb.CommandType, payload any, out any) error {
	commandID := uuid.NewString()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("remote runtime: marshal payload: %w", err)
	}

	cmd := &agentpb.Command{
		CommandId: commandID,
		Type:      cmdType,
		Payload:   payloadBytes,
	}

	// Register result channel before sending to avoid a race.
	resultCh, err := r.registry.WaitForResult(r.agentName, commandID)
	if err != nil {
		return err
	}

	// Deliver the command to the agent's CommandCh.
	entry, ok := r.registry.Get(r.agentName)
	if !ok {
		return fmt.Errorf("remote runtime: agent %s not found", r.agentName)
	}

	select {
	case entry.CommandCh <- cmd:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("remote runtime: timed out enqueuing command to agent %s", r.agentName)
	}

	// Determine effective timeout.
	timeout := commandTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 && d < timeout {
			timeout = d
		}
	}

	select {
	case res := <-resultCh:
		if !res.GetSuccess() {
			return fmt.Errorf("remote runtime: agent %s returned error: %s", r.agentName, res.GetError())
		}
		if out != nil && len(res.GetData()) > 0 {
			if err := json.Unmarshal(res.GetData(), out); err != nil {
				return fmt.Errorf("remote runtime: unmarshal result: %w", err)
			}
		}
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("remote runtime: command %s timed out after %s", commandID, timeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// payload types (JSON-encoded into Command.Payload)
// ──────────────────────────────────────────────────────────────────────────────

type listImagesPayload struct{}
type pullImagePayload struct{ Image string }
type listPodsPayload struct{ Filters map[string][]string }
type createPodPayload struct {
	Body string            `json:"body"` // JSON string of pod spec
	Opts map[string]string `json:"opts"`
}
type deletePodPayload struct {
	ID    string `json:"id"`
	Force *bool  `json:"force"`
}
type stopPodPayload struct{ ID string }
type startPodPayload struct{ ID string }
type inspectPodPayload struct{ NameOrID string }
type podExistsPayload struct{ NameOrID string }
type podLogsPayload struct{ NameOrID string }
type getPodResourcesPayload struct{ NameOrID string }
type listSecretsPayload struct{ Filters map[string][]string }
type deleteSecretPayload struct{ Name string }
type secretExistsPayload struct{ NameOrID string }
type deleteVolumePayload struct{ Name string }
type volumeExistsPayload struct{ NameOrID string }
type inspectContainerPayload struct{ NameOrID string }
type containerExistsPayload struct{ NameOrID string }
type containerLogsPayload struct{ ContainerNameOrID string }
type listRoutesPayload struct{}
type deletePVCsPayload struct{ AppLabel string }
type getSystemInfoPayload struct{}
type runEphemeralContainerPayload struct {
	Image  string            `json:"image"`
	Cmd    []string          `json:"cmd"`
	Mounts []types.BindMount `json:"mounts"`
}

// Proxy payload types – Caddy management on the remote worker.
type registerProxyRoutePayload struct {
	ID        string `json:"id"`
	Domain    string `json:"domain"`
	Upstream  string `json:"upstream"`
	Terminal  bool   `json:"terminal"`
	RouteType string `json:"type"`
}
type unregisterProxyRoutePayload struct{ RouteID string }
type getProxyRoutePayload struct{ RouteID string }
type proxyHealthCheckPayload struct{}

// httpProxyPayload is the request payload for COMMAND_TYPE_HTTP_PROXY.
type httpProxyPayload struct {
	Method    string            `json:"method"`
	TargetURL string            `json:"target_url"`
	Headers   map[string]string `json:"headers,omitempty"`
	Body      []byte            `json:"body,omitempty"`
}

// ──────────────────────────────────────────────────────────────────────────────
// runtime.Runtime interface implementation
// ──────────────────────────────────────────────────────────────────────────────

func (r *RemoteRuntime) ListImages() ([]types.Image, error) {
	var result []types.Image
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_LIST_IMAGES, listImagesPayload{}, &result)
	return result, err
}

func (r *RemoteRuntime) PullImage(image string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_PULL_IMAGE, pullImagePayload{Image: image}, nil)
}

func (r *RemoteRuntime) ListPods(filters map[string][]string) ([]types.Pod, error) {
	var result []types.Pod
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_LIST_PODS, listPodsPayload{Filters: filters}, &result)
	return result, err
}

func (r *RemoteRuntime) CreatePod(body io.Reader, opts map[string]string) ([]types.Pod, error) {
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("remote runtime: read pod body: %w", err)
	}
	var result []types.Pod
	err = r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_CREATE_POD,
		createPodPayload{Body: string(bodyBytes), Opts: opts}, &result)
	return result, err
}

func (r *RemoteRuntime) DeletePod(id string, force *bool) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_DELETE_POD, deletePodPayload{ID: id, Force: force}, nil)
}

func (r *RemoteRuntime) StopPod(id string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_STOP_POD, stopPodPayload{ID: id}, nil)
}

func (r *RemoteRuntime) StartPod(id string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_START_POD, startPodPayload{ID: id}, nil)
}

func (r *RemoteRuntime) InspectPod(nameOrID string) (*types.Pod, error) {
	var result types.Pod
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_INSPECT_POD, inspectPodPayload{NameOrID: nameOrID}, &result)
	return &result, err
}

func (r *RemoteRuntime) PodExists(nameOrID string) (bool, error) {
	var result bool
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_POD_EXISTS, podExistsPayload{NameOrID: nameOrID}, &result)
	return result, err
}

func (r *RemoteRuntime) PodLogs(nameOrID string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_POD_LOGS, podLogsPayload{NameOrID: nameOrID}, nil)
}

func (r *RemoteRuntime) GetPodResources(nameOrID string) (*types.PodResources, error) {
	var result types.PodResources
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_GET_POD_RESOURCES, getPodResourcesPayload{NameOrID: nameOrID}, &result)
	return &result, err
}

func (r *RemoteRuntime) ListSecrets(filters map[string][]string) ([]string, error) {
	var result []string
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_LIST_SECRETS, listSecretsPayload{Filters: filters}, &result)
	return result, err
}

func (r *RemoteRuntime) DeleteSecret(name string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_DELETE_SECRET, deleteSecretPayload{Name: name}, nil)
}

func (r *RemoteRuntime) SecretExists(nameOrID string) (bool, error) {
	var result bool
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_SECRET_EXISTS, secretExistsPayload{NameOrID: nameOrID}, &result)
	return result, err
}

func (r *RemoteRuntime) DeleteVolume(name string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_DELETE_VOLUME, deleteVolumePayload{Name: name}, nil)
}

func (r *RemoteRuntime) VolumeExists(nameOrID string) (bool, error) {
	var result bool
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_VOLUME_EXISTS, volumeExistsPayload{NameOrID: nameOrID}, &result)
	return result, err
}

func (r *RemoteRuntime) InspectContainer(nameOrID string) (*types.Container, error) {
	var result types.Container
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_INSPECT_CONTAINER, inspectContainerPayload{NameOrID: nameOrID}, &result)
	return &result, err
}

func (r *RemoteRuntime) ContainerExists(nameOrID string) (bool, error) {
	var result bool
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_CONTAINER_EXISTS, containerExistsPayload{NameOrID: nameOrID}, &result)
	return result, err
}

func (r *RemoteRuntime) ContainerLogs(containerNameOrID string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_CONTAINER_LOGS, containerLogsPayload{ContainerNameOrID: containerNameOrID}, nil)
}

func (r *RemoteRuntime) ListRoutes() ([]types.Route, error) {
	var result []types.Route
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_LIST_ROUTES, listRoutesPayload{}, &result)
	return result, err
}

func (r *RemoteRuntime) DeletePVCs(appLabel string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_DELETE_PVCS, deletePVCsPayload{AppLabel: appLabel}, nil)
}

func (r *RemoteRuntime) GetSystemInfo() (*models.SystemInfo, error) {
	var result models.SystemInfo
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_GET_SYSTEM_INFO, getSystemInfoPayload{}, &result)
	return &result, err
}

func (r *RemoteRuntime) RunEphemeralContainer(image string, cmd []string, mounts []types.BindMount) (int32, error) {
	var exitCode int32
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_RUN_EPHEMERAL_CONTAINER,
		runEphemeralContainerPayload{Image: image, Cmd: cmd, Mounts: mounts}, &exitCode)
	return exitCode, err
}

// WorkerIP returns the TCP source IP of the worker agent as observed by the
// gateway, or empty string if not yet known.
func (r *RemoteRuntime) WorkerIP() string {
	entry, ok := r.registry.Get(r.agentName)
	if !ok {
		return ""
	}
	return entry.WorkerIP
}

// DomainSuffix returns the domain suffix the worker computed at configure time,
// or empty string if not yet known.
func (r *RemoteRuntime) DomainSuffix() string {
	entry, ok := r.registry.Get(r.agentName)
	if !ok {
		return ""
	}
	return entry.DomainSuffix
}

// Type queries the remote agent for its local runtime type and maps it to the
// corresponding remote constant (RuntimeTypeRemotePodman / RuntimeTypeRemoteOpenShift).
// Falls back to RuntimeTypeRemotePodman on any error since all current agents run Podman.
func (r *RemoteRuntime) Type() types.RuntimeType {
	var raw string
	if err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_RUNTIME_TYPE, struct{}{}, &raw); err != nil {
		return types.RuntimeTypeRemotePodman
	}
	switch types.RuntimeType(raw) {
	case types.RuntimeTypeOpenShift:
		return types.RuntimeTypeRemoteOpenShift
	default:
		return types.RuntimeTypeRemotePodman
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Proxy operations – Caddy management on the remote worker node.
// These dispatch over gRPC so the worker's local Caddy is managed remotely.
// ──────────────────────────────────────────────────────────────────────────────

// RegisterProxyRoute dispatches a route registration to the remote worker's Caddy.
func (r *RemoteRuntime) RegisterProxyRoute(ctx context.Context, route types.ProxyRoute) error {
	return r.dispatch(ctx, agentpb.CommandType_COMMAND_TYPE_REGISTER_PROXY_ROUTE,
		registerProxyRoutePayload{
			ID:        route.ID,
			Domain:    route.Domain,
			Upstream:  route.Upstream,
			Terminal:  route.Terminal,
			RouteType: route.Type,
		}, nil)
}

// UnregisterProxyRoute dispatches a route removal to the remote worker's Caddy.
func (r *RemoteRuntime) UnregisterProxyRoute(routeID string) error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_UNREGISTER_PROXY_ROUTE,
		unregisterProxyRoutePayload{RouteID: routeID}, nil)
}

// GetProxyRoute retrieves a route by ID from the remote worker's Caddy.
func (r *RemoteRuntime) GetProxyRoute(routeID string) (*types.ProxyRoute, error) {
	var result types.ProxyRoute
	err := r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_GET_PROXY_ROUTE,
		getProxyRoutePayload{RouteID: routeID}, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// ProxyHealthCheck verifies the remote worker's Caddy instance is reachable.
func (r *RemoteRuntime) ProxyHealthCheck() error {
	return r.dispatch(context.Background(), agentpb.CommandType_COMMAND_TYPE_PROXY_HEALTH_CHECK,
		proxyHealthCheckPayload{}, nil)
}

// ──────────────────────────────────────────────────────────────────────────────
// HTTP proxy tunnel
// The control plane sends an HTTP request over the gRPC stream; the worker
// executes it locally against a pod endpoint and returns the response.
// ──────────────────────────────────────────────────────────────────────────────

// HTTPProxy tunnels an HTTP request to the worker via the gRPC stream.
// The worker daemon executes the request locally (inside the Podman network)
// and returns the status, headers, and body as a single CommandResult.
func (r *RemoteRuntime) HTTPProxy(ctx context.Context, method, targetURL string, headers map[string]string, body []byte) (*types.HTTPProxyResponse, error) {
	var result types.HTTPProxyResponse
	err := r.dispatch(ctx, agentpb.CommandType_COMMAND_TYPE_HTTP_PROXY,
		httpProxyPayload{
			Method:    method,
			TargetURL: targetURL,
			Headers:   headers,
			Body:      body,
		}, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
