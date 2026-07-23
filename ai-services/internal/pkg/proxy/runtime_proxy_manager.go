package proxy

import (
	"context"
	"fmt"

	runtimetypes "github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
)

// RuntimeProxyInterface is the subset of runtime.Runtime needed to manage
// Caddy routes on a remote worker.  Using an interface here rather
// than the full runtime.Runtime breaks the import cycle between the proxy and
// runtime packages.
type RuntimeProxyInterface interface {
	RegisterProxyRoute(ctx context.Context, route runtimetypes.ProxyRoute) error
	UnregisterProxyRoute(routeID string) error
	GetProxyRoute(routeID string) (*runtimetypes.ProxyRoute, error)
	ProxyHealthCheck() error
}

// runtimeProxyManager implements ProxyManager by delegating to the runtime's
// proxy methods.  For a RemoteRuntime every call is dispatched over the gRPC
// agent stream so the worker's local Caddy is managed remotely.
type runtimeProxyManager struct {
	ctx context.Context
	rt  RuntimeProxyInterface
}

// NewRuntimeProxyManager wraps a RuntimeProxyInterface as a ProxyManager.
// ctx is used for operations that require a context (RegisterRoute).
func NewRuntimeProxyManager(ctx context.Context, rt RuntimeProxyInterface) ProxyManager {
	return &runtimeProxyManager{ctx: ctx, rt: rt}
}

func (r *runtimeProxyManager) HealthCheck() error {
	return r.rt.ProxyHealthCheck()
}

func (r *runtimeProxyManager) RegisterRoute(ctx context.Context, route Route) error {
	return r.rt.RegisterProxyRoute(ctx, runtimetypes.ProxyRoute{
		ID:       route.ID,
		Domain:   route.Domain,
		Upstream: route.Upstream,
		Terminal: route.Terminal,
		Type:     route.Type,
	})
}

func (r *runtimeProxyManager) UnregisterRoute(routeID string) error {
	if routeID == "" {
		return fmt.Errorf("route ID cannot be empty")
	}
	return r.rt.UnregisterProxyRoute(routeID)
}

func (r *runtimeProxyManager) GetRouteByID(routeID string) (*Route, error) {
	pr, err := r.rt.GetProxyRoute(routeID)
	if err != nil {
		return nil, err
	}
	return &Route{
		ID:       pr.ID,
		Domain:   pr.Domain,
		Upstream: pr.Upstream,
		Terminal: pr.Terminal,
		Type:     pr.Type,
	}, nil
}
