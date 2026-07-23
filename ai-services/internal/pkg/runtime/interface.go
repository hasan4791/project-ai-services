package runtime

import (
	"context"
	"io"

	"github.com/project-ai-services/ai-services/internal/pkg/models"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
)

type Runtime interface {
	// Image operations
	ListImages() ([]types.Image, error)
	PullImage(image string) error

	// Pod operations
	ListPods(filters map[string][]string) ([]types.Pod, error)
	CreatePod(body io.Reader, opts map[string]string) ([]types.Pod, error)
	DeletePod(id string, force *bool) error
	StopPod(id string) error
	StartPod(id string) error
	InspectPod(nameOrId string) (*types.Pod, error)
	PodExists(nameOrID string) (bool, error)
	PodLogs(nameOrID string) error
	GetPodResources(nameOrID string) (*types.PodResources, error)

	// Secret operations
	ListSecrets(filters map[string][]string) ([]string, error)
	DeleteSecret(name string) error
	SecretExists(nameOrID string) (bool, error)

	// Volume operations
	DeleteVolume(name string) error
	VolumeExists(nameOrID string) (bool, error)

	// Container operations
	// ListContainers(filters map[string][]string) ([]types.Container, error)
	InspectContainer(nameOrId string) (*types.Container, error)
	ContainerExists(nameOrID string) (bool, error)
	ContainerLogs(containerNameOrID string) error

	// Network operations
	ListRoutes() ([]types.Route, error)

	// PVC operations
	DeletePVCs(appLabel string) error

	// System information
	GetSystemInfo() (*models.SystemInfo, error)

	// RunEphemeralContainer runs a one-shot container (image + cmd + mounts),
	// waits for it to exit, and returns its exit code.
	// For local Podman this runs via the Podman socket directly.
	// For a remote agent this is dispatched over gRPC so the container
	// runs on the worker LPAR, not the control plane.
	RunEphemeralContainer(image string, cmd []string, mounts []types.BindMount) (int32, error)

	// Proxy operations – Caddy management on the node.
	// RegisterProxyRoute registers a route with the local Caddy instance.
	RegisterProxyRoute(ctx context.Context, route types.ProxyRoute) error
	// UnregisterProxyRoute removes a route from the local Caddy instance.
	UnregisterProxyRoute(routeID string) error
	// GetProxyRoute retrieves a route by ID from the local Caddy instance.
	GetProxyRoute(routeID string) (*types.ProxyRoute, error)
	// ProxyHealthCheck verifies the local Caddy instance is reachable.
	ProxyHealthCheck() error

	// HTTPProxy tunnels an HTTP request through the gRPC stream to a worker
	// pod endpoint and returns the response.
	// method is the HTTP verb (GET, POST, …), targetURL is the full URL of the
	// pod endpoint on the worker (e.g. "http://pod-name:8080/health"),
	// headers are optional extra request headers, and body is the request body
	// (may be nil).  Returns the HTTP status code, response headers, and body.
	HTTPProxy(ctx context.Context, method, targetURL string, headers map[string]string, body []byte) (*types.HTTPProxyResponse, error)

	// Runtime type identification
	Type() types.RuntimeType
}
