// Package httpclient provides AgentHTTPClient, a thin client for the control
// plane to make HTTP requests to service endpoints running inside a remote
// worker's Podman network.
//
// Because worker pods are only reachable through the Podman network on the
// worker LPAR, the control plane cannot connect to them directly.
// AgentHTTPClient sends each request as a COMMAND_TYPE_HTTP_PROXY command
// over the existing gRPC agent stream.  The daemon executes the HTTP call
// locally on the worker and returns the response as a CommandResult.
//
// This avoids the need for a new port or a persistent proxy process.
// The gRPC stream is the only channel required.
package httpclient

import (
	"context"
	"fmt"
	"net/http"

	"github.com/project-ai-services/ai-services/internal/pkg/runtime"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
)

// AgentHTTPClient issues HTTP requests to a remote worker's pod endpoints
// through the gRPC agent stream.
type AgentHTTPClient struct {
	rt runtime.Runtime
}

// New creates an AgentHTTPClient backed by rt.
// rt should be a *remote.RemoteRuntime; for local runtimes the calls execute
// on the same machine, which is still correct.
func New(rt runtime.Runtime) *AgentHTTPClient {
	return &AgentHTTPClient{rt: rt}
}

// Get performs a GET request to the given pod endpoint URL.
// targetURL is the full URL as seen from inside the worker's Podman network,
// e.g. "http://my-app--chat-bot:8080/health".
func (c *AgentHTTPClient) Get(ctx context.Context, targetURL string) (*types.HTTPProxyResponse, error) {
	return c.Do(ctx, http.MethodGet, targetURL, nil, nil)
}

// Post performs a POST request with a JSON body to the given pod endpoint URL.
func (c *AgentHTTPClient) Post(ctx context.Context, targetURL string, body []byte) (*types.HTTPProxyResponse, error) {
	return c.Do(ctx, http.MethodPost, targetURL, map[string]string{
		"Content-Type": "application/json",
	}, body)
}

// Do performs an arbitrary HTTP request through the agent stream.
// method is the HTTP verb (GET, POST, …).
// headers are merged on top of any default headers.
// body may be nil for requests without a body.
func (c *AgentHTTPClient) Do(ctx context.Context, method, targetURL string, headers map[string]string, body []byte) (*types.HTTPProxyResponse, error) {
	resp, err := c.rt.HTTPProxy(ctx, method, targetURL, headers, body)
	if err != nil {
		return nil, fmt.Errorf("agent http client: %w", err)
	}
	return resp, nil
}

// IsSuccess returns true when the response status code is 2xx.
func IsSuccess(resp *types.HTTPProxyResponse) bool {
	return resp != nil && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
}
