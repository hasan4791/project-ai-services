// Package dispatcher selects a worker agent from the registry and returns a
// runtime.Runtime backed by that agent (RemoteRuntime).
package dispatcher

import (
	"fmt"

	agentregistry "github.com/project-ai-services/ai-services/internal/pkg/agent/registry"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/remote"
)

// AgentDispatcher selects an available agent and wraps it in a RemoteRuntime.
type AgentDispatcher struct {
	registry *agentregistry.Registry
}

// New creates a new AgentDispatcher backed by the given registry.
func New(reg *agentregistry.Registry) *AgentDispatcher {
	return &AgentDispatcher{registry: reg}
}

// SelectAgent picks a READY agent that matches selector labels and returns a
// runtime.Runtime that proxies all calls to that agent.
//
// selector is an optional map of label key/value pairs; pass nil to pick any
// available agent.
func (d *AgentDispatcher) SelectAgent(selector map[string]string) (runtime.Runtime, string, error) {
	if selector == nil {
		selector = map[string]string{}
	}

	entry, err := d.registry.SelectAgent(selector)
	if err != nil {
		return nil, "", fmt.Errorf("dispatcher: no agent available: %w", err)
	}

	rt := remote.New(entry.AgentName, d.registry)
	return rt, entry.AgentName, nil
}
