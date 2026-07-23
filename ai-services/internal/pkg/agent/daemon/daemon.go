// Package daemon implements the worker agent daemon that runs on each LPAR.
// It connects to the control plane AgentGateway over a bidirectional gRPC stream
// and executes runtime commands on behalf of the control plane.
package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	agentpb "github.com/project-ai-services/ai-services/internal/pkg/agent/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime"
	runtimetypes "github.com/project-ai-services/ai-services/internal/pkg/runtime/types"
)

const (
	heartbeatInterval  = 30 * time.Second
	reconnectBaseDelay = 5 * time.Second
	reconnectMaxDelay  = 120 * time.Second
)

// Config holds the configuration for the agent daemon.
type Config struct {
	AgentName       string
	ControlPlaneURL string // e.g. "lpar-0.example.com:9090"
	PreSharedToken  string
	Labels          map[string]string
	Capabilities    map[string]string
}

// Daemon is the worker agent daemon.
type Daemon struct {
	cfg Config
	rt  runtime.Runtime // injected at construction; determines what commands are dispatched to
}

// New creates a new Daemon with the provided config and runtime.
// The runtime is used for all command dispatches and its Type() is reported
// back to the control plane via COMMAND_TYPE_RUNTIME_TYPE.
func New(cfg Config, rt runtime.Runtime) *Daemon {
	return &Daemon{cfg: cfg, rt: rt}
}

// Run starts the daemon and blocks until ctx is cancelled.
// Registration must have been performed by the caller before Run is invoked
// (i.e. agentbootstrap.Register succeeded). Run only maintains the CommandStream,
// reconnecting with exponential backoff on disconnection.
func (d *Daemon) Run(ctx context.Context) error {
	logger.InfofCtx(ctx, "agent daemon starting: agent_name=%s control_plane=%s runtime=%s",
		d.cfg.AgentName, d.cfg.ControlPlaneURL, d.rt.Type())

	conn, err := grpc.NewClient(d.cfg.ControlPlaneURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("agent daemon: dial %s: %w", d.cfg.ControlPlaneURL, err)
	}
	defer conn.Close()

	client := agentpb.NewAgentGatewayClient(conn)

	// Maintain the CommandStream with reconnect loop.
	delay := reconnectBaseDelay
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := d.runStream(ctx, client); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.WarningfCtx(ctx, "agent daemon: stream error (%v), reconnecting in %s", err, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
			if delay < reconnectMaxDelay {
				delay *= 2
			}
			continue
		}
		delay = reconnectBaseDelay
	}
}

// runStream opens the bidirectional CommandStream, sends a heartbeat as the
// first message, then continuously processes incoming Commands.
func (d *Daemon) runStream(ctx context.Context, client agentpb.AgentGatewayClient) error {
	stream, err := client.CommandStream(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	// Send initial heartbeat so the gateway can identify us.
	if err := stream.Send(&agentpb.CommandResult{
		AgentName:   d.cfg.AgentName,
		IsHeartbeat: true,
		Success:     true,
	}); err != nil {
		return fmt.Errorf("send initial heartbeat: %w", err)
	}

	// Start the heartbeat goroutine.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go d.heartbeatLoop(hbCtx, stream)

	// Process incoming Commands.
	for {
		cmd, err := stream.Recv()
		if err == io.EOF {
			return fmt.Errorf("stream closed by server")
		}
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}

		result := d.executeCommand(ctx, cmd)
		if err := stream.Send(result); err != nil {
			return fmt.Errorf("send result: %w", err)
		}
	}
}

// heartbeatLoop sends a heartbeat every heartbeatInterval until ctx is done.
func (d *Daemon) heartbeatLoop(ctx context.Context, stream agentpb.AgentGateway_CommandStreamClient) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = stream.Send(&agentpb.CommandResult{
				AgentName:   d.cfg.AgentName,
				IsHeartbeat: true,
				Success:     true,
			})
		}
	}
}

// executeCommand dispatches a Command to the injected runtime and returns
// the CommandResult.
func (d *Daemon) executeCommand(ctx context.Context, cmd *agentpb.Command) *agentpb.CommandResult {
	result, err := d.dispatchToRuntime(ctx, cmd)

	r := &agentpb.CommandResult{
		CommandId: cmd.GetCommandId(),
		AgentName: d.cfg.AgentName,
	}
	if err != nil {
		r.Success = false
		r.Error = err.Error()
	} else {
		r.Success = true
		r.Data = result
	}
	return r
}

// dispatchToRuntime dispatches the command to d.rt (the injected runtime).
func (d *Daemon) dispatchToRuntime(ctx context.Context, cmd *agentpb.Command) ([]byte, error) {
	rt := d.rt

	switch cmd.GetType() {
	case agentpb.CommandType_COMMAND_TYPE_LIST_IMAGES:
		result, err := rt.ListImages()
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_PULL_IMAGE:
		var p struct{ Image string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.PullImage(p.Image)

	case agentpb.CommandType_COMMAND_TYPE_LIST_PODS:
		var p struct{ Filters map[string][]string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.ListPods(p.Filters)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_CREATE_POD:
		var p struct {
			Body string            `json:"body"`
			Opts map[string]string `json:"opts"`
		}
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.CreatePod(strings.NewReader(p.Body), p.Opts)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_DELETE_POD:
		var p struct {
			ID    string `json:"id"`
			Force *bool  `json:"force"`
		}
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.DeletePod(p.ID, p.Force)

	case agentpb.CommandType_COMMAND_TYPE_STOP_POD:
		var p struct{ ID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.StopPod(p.ID)

	case agentpb.CommandType_COMMAND_TYPE_START_POD:
		var p struct{ ID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.StartPod(p.ID)

	case agentpb.CommandType_COMMAND_TYPE_INSPECT_POD:
		var p struct{ NameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.InspectPod(p.NameOrID)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_POD_EXISTS:
		var p struct{ NameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.PodExists(p.NameOrID)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_POD_LOGS:
		var p struct{ NameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.PodLogs(p.NameOrID)

	case agentpb.CommandType_COMMAND_TYPE_GET_POD_RESOURCES:
		var p struct{ NameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.GetPodResources(p.NameOrID)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_LIST_SECRETS:
		var p struct{ Filters map[string][]string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.ListSecrets(p.Filters)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_DELETE_SECRET:
		var p struct{ Name string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.DeleteSecret(p.Name)

	case agentpb.CommandType_COMMAND_TYPE_SECRET_EXISTS:
		var p struct{ NameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.SecretExists(p.NameOrID)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_DELETE_VOLUME:
		var p struct{ Name string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.DeleteVolume(p.Name)

	case agentpb.CommandType_COMMAND_TYPE_VOLUME_EXISTS:
		var p struct{ NameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.VolumeExists(p.NameOrID)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_INSPECT_CONTAINER:
		var p struct{ NameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.InspectContainer(p.NameOrID)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_CONTAINER_EXISTS:
		var p struct{ NameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.ContainerExists(p.NameOrID)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_CONTAINER_LOGS:
		var p struct{ ContainerNameOrID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.ContainerLogs(p.ContainerNameOrID)

	case agentpb.CommandType_COMMAND_TYPE_LIST_ROUTES:
		result, err := rt.ListRoutes()
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_DELETE_PVCS:
		var p struct{ AppLabel string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.DeletePVCs(p.AppLabel)

	case agentpb.CommandType_COMMAND_TYPE_GET_SYSTEM_INFO:
		result, err := rt.GetSystemInfo()
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_RUNTIME_TYPE:
		// Report the actual runtime type so the control plane can route
		// to the correct deployer (PodmanDeployer, OpenShiftDeployer, etc.).
		return marshalResult(string(rt.Type()), nil)

	case agentpb.CommandType_COMMAND_TYPE_RUN_EPHEMERAL_CONTAINER:
		var p struct {
			Image  string                   `json:"image"`
			Cmd    []string                 `json:"cmd"`
			Mounts []runtimetypes.BindMount `json:"mounts"`
		}
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		exitCode, err := rt.RunEphemeralContainer(p.Image, p.Cmd, p.Mounts)
		return marshalResult(exitCode, err)

	// ── Caddy proxy management on the worker node ──────────────────

	case agentpb.CommandType_COMMAND_TYPE_REGISTER_PROXY_ROUTE:
		var p struct {
			ID        string `json:"id"`
			Domain    string `json:"domain"`
			Upstream  string `json:"upstream"`
			Terminal  bool   `json:"terminal"`
			RouteType string `json:"type"`
		}
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		err := rt.RegisterProxyRoute(ctx, runtimetypes.ProxyRoute{
			ID:       p.ID,
			Domain:   p.Domain,
			Upstream: p.Upstream,
			Terminal: p.Terminal,
			Type:     p.RouteType,
		})
		return nil, err

	case agentpb.CommandType_COMMAND_TYPE_UNREGISTER_PROXY_ROUTE:
		var p struct{ RouteID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		return nil, rt.UnregisterProxyRoute(p.RouteID)

	case agentpb.CommandType_COMMAND_TYPE_GET_PROXY_ROUTE:
		var p struct{ RouteID string }
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.GetProxyRoute(p.RouteID)
		return marshalResult(result, err)

	case agentpb.CommandType_COMMAND_TYPE_PROXY_HEALTH_CHECK:
		return nil, rt.ProxyHealthCheck()

	// ── HTTP proxy tunnel ──────────────────────────────────────────

	case agentpb.CommandType_COMMAND_TYPE_HTTP_PROXY:
		var p struct {
			Method    string            `json:"method"`
			TargetURL string            `json:"target_url"`
			Headers   map[string]string `json:"headers,omitempty"`
			Body      []byte            `json:"body,omitempty"`
		}
		if err := json.Unmarshal(cmd.GetPayload(), &p); err != nil {
			return nil, err
		}
		result, err := rt.HTTPProxy(ctx, p.Method, p.TargetURL, p.Headers, p.Body)
		return marshalResult(result, err)

	default:
		return nil, fmt.Errorf("unknown command type: %v", cmd.GetType())
	}
}

func marshalResult(v any, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
