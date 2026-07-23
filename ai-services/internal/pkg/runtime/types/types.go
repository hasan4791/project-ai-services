package types

import "time"

// RuntimeType represents the type of container runtime.
type RuntimeType string

const (
	// Local runtimes — used directly on the control plane LPAR.
	RuntimeTypePodman    RuntimeType = "podman"
	RuntimeTypeOpenShift RuntimeType = "openshift"

	// Remote runtimes — resolved by querying the agent via COMMAND_TYPE_RUNTIME_TYPE.
	// The agent daemon returns its local runtime type ("podman" or "openshift");
	// RemoteRuntime.Type() maps that to one of these two constants so the executor
	// can route to the correct deployer without any string parsing in call sites.
	RuntimeTypeRemotePodman    RuntimeType = "remote-podman"
	RuntimeTypeRemoteOpenShift RuntimeType = "remote-openshift"
)

// String returns the string representation of RuntimeType.
func (r RuntimeType) String() string {
	return string(r)
}

// Valid checks if the runtime type is valid.
func (r RuntimeType) Valid() bool {
	switch r {
	case RuntimeTypePodman, RuntimeTypeOpenShift,
		RuntimeTypeRemotePodman, RuntimeTypeRemoteOpenShift:
		return true
	default:
		return false
	}
}

type Pod struct {
	ID               string
	Name             string
	Status           string
	Health           string
	Labels           map[string]string
	Containers       []Container
	Created          time.Time
	Ports            map[string][]string
	State            string
	InfraContainerID string
}

type Container struct {
	ID                     string `json:"ID"`
	Name                   string
	Status                 string
	Health                 string
	Annotations            map[string]string
	Env                    map[string]string
	HealthcheckStartPeriod time.Duration
}

type Image struct {
	RepoTags    []string
	RepoDigests []string
}

type Route struct {
	Name       string
	HostPort   string
	TargetPort string
}

// PodResources represents resource allocation and usage for a pod including accelerators.
type PodResources struct {
	CPU        float64  // CPU usage (e.g., 1.5 CPUs)
	MemUsage   uint64   // Memory usage in bytes
	SpyreCards []string // List of Spyre card PCI addresses
}

// BindMount describes a host→container bind mount for an ephemeral container.
type BindMount struct {
	Source      string   // host path
	Destination string   // container path
	Options     []string // e.g. ["Z"]
}

// HTTPProxyResponse carries the result of an HTTP request executed on the
// worker node and returned to the control plane via the gRPC stream.
type HTTPProxyResponse struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
}

// ProxyRoute represents a reverse proxy route for the worker node's Caddy
// instance.  Kept in runtime/types so the Runtime interface can
// reference it without importing the proxy package (which would cause a cycle).
type ProxyRoute struct {
	// ID is the unique identifier for the route (used as @id in Caddy config).
	ID string
	// Domain is the hostname to match (e.g. "service.example.com").
	Domain string
	// Upstream is the backend address (e.g. "pod-name:8080").
	Upstream string
	// Terminal stops route matching after this route.
	Terminal bool
	// Type is the endpoint type label (e.g. "ui", "api").
	Type string
}
