package proxy

import (
	"context"
	"fmt"

	runtimetypes "github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
)

// LocalCaddyManagerAdapter wraps a ProxyManager to satisfy the
// podman.LocalCaddyManager interface.  This adapter lives in the proxy package
// so that callers (daemon, agent start) can create it without the podman
// package needing to import proxy directly.
type LocalCaddyManagerAdapter struct {
	pm ProxyManager
}

// NewLocalCaddyManagerAdapter creates an adapter from an existing ProxyManager.
func NewLocalCaddyManagerAdapter(pm ProxyManager) *LocalCaddyManagerAdapter {
	return &LocalCaddyManagerAdapter{pm: pm}
}

// NewLocalCaddyManagerFromEnv creates an adapter using the CADDY_ADMIN_URL env var.
// Returns an error if the env var is not set.
func NewLocalCaddyManagerFromEnv() (*LocalCaddyManagerAdapter, error) {
	pm, err := GetCaddyProxyManager()
	if err != nil {
		return nil, fmt.Errorf("local caddy manager: %w", err)
	}
	return NewLocalCaddyManagerAdapter(pm), nil
}

// RegisterRoute implements podman.LocalCaddyManager.
func (a *LocalCaddyManagerAdapter) RegisterRoute(ctx context.Context, id, domain, upstream string, terminal bool, routeType string) error {
	return a.pm.RegisterRoute(ctx, Route{
		ID:       id,
		Domain:   domain,
		Upstream: upstream,
		Terminal: terminal,
		Type:     routeType,
	})
}

// UnregisterRoute implements podman.LocalCaddyManager.
func (a *LocalCaddyManagerAdapter) UnregisterRoute(routeID string) error {
	return a.pm.UnregisterRoute(routeID)
}

// GetRoute implements podman.LocalCaddyManager.
func (a *LocalCaddyManagerAdapter) GetRoute(routeID string) (*runtimetypes.ProxyRoute, error) {
	r, err := a.pm.GetRouteByID(routeID)
	if err != nil {
		return nil, err
	}
	return &runtimetypes.ProxyRoute{
		ID:       r.ID,
		Domain:   r.Domain,
		Upstream: r.Upstream,
		Terminal: r.Terminal,
		Type:     r.Type,
	}, nil
}

// HealthCheck implements podman.LocalCaddyManager.
func (a *LocalCaddyManagerAdapter) HealthCheck() error {
	return a.pm.HealthCheck()
}
