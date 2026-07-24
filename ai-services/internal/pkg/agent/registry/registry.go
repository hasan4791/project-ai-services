// Package registry manages the state of all registered remote worker agents.
// It maintains an in-memory map for fast look-ups and persists state to PostgreSQL.
package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	agentpb "github.com/project-ai-services/ai-services/internal/pkg/agent/proto"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
)

// AgentStatus mirrors the DB enum values.
type AgentStatus string

const (
	AgentStatusPending      AgentStatus = "pending"
	AgentStatusReady        AgentStatus = "ready"
	AgentStatusBusy         AgentStatus = "busy"
	AgentStatusDraining     AgentStatus = "draining"
	AgentStatusDisconnected AgentStatus = "disconnected"
	AgentStatusRejected     AgentStatus = "rejected"
)

const (
	// heartbeatTimeout is how long since the last heartbeat before an agent is
	// considered disconnected.
	heartbeatTimeout = 90 * time.Second

	// heartbeatWatchInterval is how often the watcher sweeps all agents.
	heartbeatWatchInterval = 30 * time.Second
)

// AgentEntry is the in-memory record for a connected agent.
type AgentEntry struct {
	AgentName     string
	Labels        map[string]string
	Capabilities  map[string]string
	Status        AgentStatus
	LastHeartbeat time.Time
	RegisteredAt  time.Time

	// WorkerIP is the TCP source IP observed when the agent opens its
	// CommandStream.  Set by the gateway from the gRPC peer info.
	WorkerIP string

	// DomainSuffix is the computed domain suffix for route registration on this
	// worker (e.g. "192.168.1.5.nip.io" or "example.com").
	// Sent by the agent at start-up via the RegisterRequest labels.
	DomainSuffix string

	// CommandCh is written by RemoteRuntime to send commands to this agent.
	// The gateway goroutine reads from it and writes to the gRPC stream.
	CommandCh chan *agentpb.Command

	resultsMu sync.Mutex
	results   map[string]chan *agentpb.CommandResult
}

// waitForResult registers a result channel for commandID and returns it.
func (a *AgentEntry) waitForResult(commandID string) chan *agentpb.CommandResult {
	ch := make(chan *agentpb.CommandResult, 1)
	a.resultsMu.Lock()
	a.results[commandID] = ch
	a.resultsMu.Unlock()
	return ch
}

// deliverResult routes an incoming result to the waiting caller.
func (a *AgentEntry) deliverResult(res *agentpb.CommandResult) {
	id := res.GetCommandId()
	a.resultsMu.Lock()
	ch, ok := a.results[id]
	if ok {
		delete(a.results, id)
	}
	a.resultsMu.Unlock()
	if ok {
		select {
		case ch <- res:
		default:
		}
	}
}

// activeSlots returns the number of in-flight commands.
func (a *AgentEntry) activeSlots() int {
	a.resultsMu.Lock()
	defer a.resultsMu.Unlock()
	return len(a.results)
}

// Registry tracks all registered worker agents.
type Registry struct {
	mu     sync.RWMutex
	agents map[string]*AgentEntry
	pool   *pgxpool.Pool // may be nil in no-DB / test mode
}

// New creates a new Registry, optionally backed by a PostgreSQL pool.
func New(pool *pgxpool.Pool) *Registry {
	return &Registry{
		agents: make(map[string]*AgentEntry),
		pool:   pool,
	}
}

// StartHeartbeatWatcher starts a background goroutine that marks agents
// DISCONNECTED when their last heartbeat is older than heartbeatTimeout.
// It stops when ctx is cancelled.
func (r *Registry) StartHeartbeatWatcher(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(heartbeatWatchInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.sweepStaleAgents(ctx)
			}
		}
	}()
}

// sweepStaleAgents transitions any READY/BUSY agent with a stale heartbeat to DISCONNECTED.
func (r *Registry) sweepStaleAgents(ctx context.Context) {
	now := time.Now()

	r.mu.Lock()
	var stale []string
	for id, e := range r.agents {
		if e.Status != AgentStatusReady && e.Status != AgentStatusBusy {
			continue
		}
		if e.LastHeartbeat.IsZero() || now.Sub(e.LastHeartbeat) > heartbeatTimeout {
			stale = append(stale, id)
		}
	}
	for _, id := range stale {
		r.agents[id].Status = AgentStatusDisconnected
	}
	r.mu.Unlock()

	for _, id := range stale {
		logger.WarningfCtx(ctx, "agent registry: agent %s heartbeat timed out — marked DISCONNECTED", id)
		if r.pool != nil {
			r.updateStatusDB(ctx, id, AgentStatusDisconnected)
		}
	}
}

// Upsert registers or updates an agent in both in-memory store and DB.
func (r *Registry) Upsert(ctx context.Context, req *agentpb.RegisterRequest) (*AgentEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	agentName := req.GetAgentName()
	entry, exists := r.agents[agentName]
	if !exists {
		entry = &AgentEntry{
			AgentName:    agentName,
			RegisteredAt: time.Now(),
			CommandCh:    make(chan *agentpb.Command, 32),
			results:      make(map[string]chan *agentpb.CommandResult),
		}
		r.agents[agentName] = entry
	}

	entry.Labels = req.GetLabels()
	entry.Capabilities = req.GetCapabilities()
	entry.Status = AgentStatusPending
	entry.LastHeartbeat = time.Now()

	if r.pool != nil {
		if err := r.upsertDB(ctx, entry); err != nil {
			logger.WarningfCtx(ctx, "agent registry: DB upsert failed for %s: %v", agentName, err)
		}
	}

	return entry, nil
}

// MarkReady transitions an agent to READY status.
func (r *Registry) MarkReady(ctx context.Context, agentName string) {
	r.updateStatus(ctx, agentName, AgentStatusReady)
}

// MarkDisconnected transitions an agent to DISCONNECTED status.
func (r *Registry) MarkDisconnected(ctx context.Context, agentName string) {
	r.updateStatus(ctx, agentName, AgentStatusDisconnected)
}

// UpdateHeartbeat refreshes the last_heartbeat timestamp for the agent.
func (r *Registry) UpdateHeartbeat(ctx context.Context, agentName string) {
	r.mu.Lock()
	entry, ok := r.agents[agentName]
	if ok {
		entry.LastHeartbeat = time.Now()
	}
	r.mu.Unlock()
	if ok && r.pool != nil {
		r.updateHeartbeatDB(ctx, agentName)
	}
}

// SelectAgent picks the best available READY agent matching the label selector.
// The reserved key "agent_name" matches directly against the agent's registered name,
// so callers can target a specific worker without requiring a custom label.
func (r *Registry) SelectAgent(selector map[string]string) (*AgentEntry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, entry := range r.agents {
		if entry.Status != AgentStatusReady {
			continue
		}
		if time.Since(entry.LastHeartbeat) > heartbeatTimeout {
			continue
		}
		if !agentMatches(entry, selector) {
			continue
		}
		return entry, nil
	}

	return nil, fmt.Errorf("no available agent matching selector %v", selector)
}

// agentMatches reports whether entry satisfies every key in selector.
// The key "agent_name" is matched against the agent's registered name directly;
// all other keys are matched against the agent's label map.
func agentMatches(entry *AgentEntry, selector map[string]string) bool {
	for k, v := range selector {
		if k == "agent_name" {
			if entry.AgentName != v {
				return false
			}
		} else {
			if entry.Labels[k] != v {
				return false
			}
		}
	}
	return true
}

// Get returns the in-memory entry for an agent.
func (r *Registry) Get(agentName string) (*AgentEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.agents[agentName]
	return e, ok
}

// SetWorkerIP stores the observed peer IP for the named agent.
func (r *Registry) SetWorkerIP(agentName, ip string) {
	r.mu.Lock()
	if e, ok := r.agents[agentName]; ok {
		e.WorkerIP = ip
	}
	r.mu.Unlock()
}

// SetDomainSuffix stores the domain suffix for route registration on this worker.
func (r *Registry) SetDomainSuffix(agentName, suffix string) {
	r.mu.Lock()
	if e, ok := r.agents[agentName]; ok {
		e.DomainSuffix = suffix
	}
	r.mu.Unlock()
}

// DeliverResult routes an incoming CommandResult to the waiting RemoteRuntime call.
func (r *Registry) DeliverResult(res *agentpb.CommandResult) {
	r.mu.RLock()
	entry, ok := r.agents[res.GetAgentName()]
	r.mu.RUnlock()
	if ok {
		entry.deliverResult(res)
	}
}

// WaitForResult returns a channel that will receive the result for commandID on agentName.
func (r *Registry) WaitForResult(agentName, commandID string) (chan *agentpb.CommandResult, error) {
	r.mu.RLock()
	entry, ok := r.agents[agentName]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("agent %s not found in registry", agentName)
	}
	return entry.waitForResult(commandID), nil
}

// AgentStatusInfo is a lightweight snapshot for CLI status output.
type AgentStatusInfo struct {
	AgentName     string
	Status        AgentStatus
	Labels        map[string]string
	LastHeartbeat time.Time
	RegisteredAt  time.Time
	ActiveSlots   int
}

// Snapshot returns a status snapshot of all known agents.
func (r *Registry) Snapshot() []AgentStatusInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]AgentStatusInfo, 0, len(r.agents))
	for _, e := range r.agents {
		out = append(out, AgentStatusInfo{
			AgentName:     e.AgentName,
			Status:        e.Status,
			Labels:        e.Labels,
			LastHeartbeat: e.LastHeartbeat,
			RegisteredAt:  e.RegisteredAt,
			ActiveSlots:   e.activeSlots(),
		})
	}
	return out
}

// Delete removes an agent from the in-memory registry and the database.
func (r *Registry) Delete(ctx context.Context, agentName string) error {
	r.mu.Lock()
	_, ok := r.agents[agentName]
	if ok {
		delete(r.agents, agentName)
	}
	r.mu.Unlock()

	if !ok {
		return fmt.Errorf("agent %s not found", agentName)
	}

	if r.pool != nil {
		if _, err := r.pool.Exec(ctx, `DELETE FROM agents WHERE agent_name = $1`, agentName); err != nil {
			logger.WarningfCtx(ctx, "agent registry: DB delete failed for %s: %v", agentName, err)
		}
	}

	return nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ──────────────────────────────────────────────────────────────────────────────

func (r *Registry) updateStatus(ctx context.Context, agentName string, status AgentStatus) {
	r.mu.Lock()
	entry, ok := r.agents[agentName]
	if ok {
		entry.Status = status
	}
	r.mu.Unlock()
	if ok && r.pool != nil {
		r.updateStatusDB(ctx, agentName, status)
	}
}


// ──────────────────────────────────────────────────────────────────────────────
// PostgreSQL persistence
// ──────────────────────────────────────────────────────────────────────────────

const upsertAgentSQL = `
INSERT INTO agents (agent_name, labels, capabilities, status, last_heartbeat, registered_at, updated_at)
VALUES ($1, $2::jsonb, $3::jsonb, $4, NOW(), NOW(), NOW())
ON CONFLICT (agent_name) DO UPDATE
  SET labels         = EXCLUDED.labels,
      capabilities   = EXCLUDED.capabilities,
      status         = EXCLUDED.status,
      last_heartbeat = NOW(),
      updated_at     = NOW()
`

func (r *Registry) upsertDB(ctx context.Context, e *AgentEntry) error {
	_, err := r.pool.Exec(ctx, upsertAgentSQL,
		e.AgentName,
		mapToJSONB(e.Labels),
		mapToJSONB(e.Capabilities),
		string(e.Status),
	)
	return err
}

const updateStatusSQL = `UPDATE agents SET status = $2, updated_at = NOW() WHERE agent_name = $1`

func (r *Registry) updateStatusDB(ctx context.Context, agentName string, status AgentStatus) {
	if _, err := r.pool.Exec(ctx, updateStatusSQL, agentName, string(status)); err != nil {
		logger.WarningfCtx(ctx, "agent registry: DB status update failed for %s: %v", agentName, err)
	}
}

const updateHeartbeatSQL = `UPDATE agents SET last_heartbeat = NOW(), updated_at = NOW() WHERE agent_name = $1`

func (r *Registry) updateHeartbeatDB(ctx context.Context, agentName string) {
	if _, err := r.pool.Exec(ctx, updateHeartbeatSQL, agentName); err != nil {
		logger.WarningfCtx(context.Background(), "agent registry: DB heartbeat update failed for %s: %v", agentName, err)
	}
}

// mapToJSONB converts a string map to a minimal JSON object string for pgx JSONB.
func mapToJSONB(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	out := "{"
	first := true
	for k, v := range m {
		if !first {
			out += ","
		}
		out += fmt.Sprintf("%q:%q", k, v)
		first = false
	}
	return out + "}"
}

// ──────────────────────────────────────────────────────────────────────────────
// Bootstrap token store
// ──────────────────────────────────────────────────────────────────────────────

// TokenRecord holds a single-use bootstrap token.
// Tokens are not pre-bound to an agent name; the name is supplied by the
// agent itself at registration time (RegisterRequest.AgentName).
type TokenRecord struct {
	Token     string
	ExpiresAt time.Time
	Used      bool
}

// TokenStore is an in-memory single-use bootstrap token store.
type TokenStore struct {
	mu     sync.Mutex
	tokens map[string]*TokenRecord
}

// NewTokenStore creates an empty token store.
func NewTokenStore() *TokenStore {
	return &TokenStore{tokens: make(map[string]*TokenRecord)}
}

// IssueToken generates a new 24-hour single-use token and returns it.
// The token is not bound to any agent name; the name is provided by the
// agent itself when it calls Register.
func (ts *TokenStore) IssueToken() string {
	token := uuid.NewString()
	ts.mu.Lock()
	ts.tokens[token] = &TokenRecord{
		Token:     token,
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}
	ts.mu.Unlock()
	return token
}

// Validate checks token validity and marks it used. Returns an error if the
// token is unknown, already used, or expired.
func (ts *TokenStore) Validate(token string) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	rec, ok := ts.tokens[token]
	if !ok {
		return fmt.Errorf("bootstrap token not found")
	}
	if rec.Used {
		return fmt.Errorf("bootstrap token already used")
	}
	if time.Now().After(rec.ExpiresAt) {
		return fmt.Errorf("bootstrap token expired")
	}
	rec.Used = true
	return nil
}
