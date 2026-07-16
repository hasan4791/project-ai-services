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

// heartbeatTimeout is how long before an agent is considered disconnected.
const heartbeatTimeout = 90 * time.Second

// AgentEntry is the in-memory record for a connected agent.
type AgentEntry struct {
	AgentID      string
	Labels       map[string]string
	Capabilities map[string]string
	Status       AgentStatus
	LastHeartbeat time.Time
	RegisteredAt  time.Time

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

// Upsert registers or updates an agent in both in-memory store and DB.
func (r *Registry) Upsert(ctx context.Context, req *agentpb.RegisterRequest) (*AgentEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	agentID := req.GetAgentId()
	entry, exists := r.agents[agentID]
	if !exists {
		entry = &AgentEntry{
			AgentID:      agentID,
			RegisteredAt: time.Now(),
			CommandCh:    make(chan *agentpb.Command, 32),
			results:      make(map[string]chan *agentpb.CommandResult),
		}
		r.agents[agentID] = entry
	}

	entry.Labels = req.GetLabels()
	entry.Capabilities = req.GetCapabilities()
	entry.Status = AgentStatusPending
	entry.LastHeartbeat = time.Now()

	if r.pool != nil {
		if err := r.upsertDB(ctx, entry); err != nil {
			logger.WarningfCtx(ctx, "agent registry: DB upsert failed for %s: %v", agentID, err)
		}
	}

	return entry, nil
}

// MarkReady transitions an agent to READY status.
func (r *Registry) MarkReady(ctx context.Context, agentID string) {
	r.updateStatus(ctx, agentID, AgentStatusReady)
}

// MarkDisconnected transitions an agent to DISCONNECTED status.
func (r *Registry) MarkDisconnected(ctx context.Context, agentID string) {
	r.updateStatus(ctx, agentID, AgentStatusDisconnected)
}

// UpdateHeartbeat refreshes the last_heartbeat timestamp for the agent.
func (r *Registry) UpdateHeartbeat(ctx context.Context, agentID string) {
	r.mu.Lock()
	entry, ok := r.agents[agentID]
	if ok {
		entry.LastHeartbeat = time.Now()
	}
	r.mu.Unlock()
	if ok && r.pool != nil {
		r.updateHeartbeatDB(ctx, agentID)
	}
}

// SelectAgent picks the best available READY agent matching the label selector.
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
		if labelsMatch(entry.Labels, selector) {
			return entry, nil
		}
	}

	return nil, fmt.Errorf("no available agent matching selector %v", selector)
}

// Get returns the in-memory entry for an agent.
func (r *Registry) Get(agentID string) (*AgentEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.agents[agentID]
	return e, ok
}

// DeliverResult routes an incoming CommandResult to the waiting RemoteRuntime call.
func (r *Registry) DeliverResult(res *agentpb.CommandResult) {
	r.mu.RLock()
	entry, ok := r.agents[res.GetAgentId()]
	r.mu.RUnlock()
	if ok {
		entry.deliverResult(res)
	}
}

// WaitForResult returns a channel that will receive the result for commandID on agentID.
func (r *Registry) WaitForResult(agentID, commandID string) (chan *agentpb.CommandResult, error) {
	r.mu.RLock()
	entry, ok := r.agents[agentID]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("agent %s not found in registry", agentID)
	}
	return entry.waitForResult(commandID), nil
}

// AgentStatusInfo is a lightweight snapshot for CLI status output.
type AgentStatusInfo struct {
	AgentID       string
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
			AgentID:       e.AgentID,
			Status:        e.Status,
			Labels:        e.Labels,
			LastHeartbeat: e.LastHeartbeat,
			RegisteredAt:  e.RegisteredAt,
			ActiveSlots:   e.activeSlots(),
		})
	}
	return out
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ──────────────────────────────────────────────────────────────────────────────

func (r *Registry) updateStatus(ctx context.Context, agentID string, status AgentStatus) {
	r.mu.Lock()
	entry, ok := r.agents[agentID]
	if ok {
		entry.Status = status
	}
	r.mu.Unlock()
	if ok && r.pool != nil {
		r.updateStatusDB(ctx, agentID, status)
	}
}

func labelsMatch(agentLabels, selector map[string]string) bool {
	for k, v := range selector {
		if agentLabels[k] != v {
			return false
		}
	}
	return true
}

// ──────────────────────────────────────────────────────────────────────────────
// PostgreSQL persistence
// ──────────────────────────────────────────────────────────────────────────────

const upsertAgentSQL = `
INSERT INTO agents (agent_id, labels, capabilities, status, last_heartbeat, registered_at, updated_at)
VALUES ($1, $2::jsonb, $3::jsonb, $4, NOW(), NOW(), NOW())
ON CONFLICT (agent_id) DO UPDATE
  SET labels         = EXCLUDED.labels,
      capabilities   = EXCLUDED.capabilities,
      status         = EXCLUDED.status,
      last_heartbeat = NOW(),
      updated_at     = NOW()
`

func (r *Registry) upsertDB(ctx context.Context, e *AgentEntry) error {
	_, err := r.pool.Exec(ctx, upsertAgentSQL,
		e.AgentID,
		mapToJSONB(e.Labels),
		mapToJSONB(e.Capabilities),
		string(e.Status),
	)
	return err
}

const updateStatusSQL = `UPDATE agents SET status = $2, updated_at = NOW() WHERE agent_id = $1`

func (r *Registry) updateStatusDB(ctx context.Context, agentID string, status AgentStatus) {
	if _, err := r.pool.Exec(ctx, updateStatusSQL, agentID, string(status)); err != nil {
		logger.WarningfCtx(ctx, "agent registry: DB status update failed for %s: %v", agentID, err)
	}
}

const updateHeartbeatSQL = `UPDATE agents SET last_heartbeat = NOW(), updated_at = NOW() WHERE agent_id = $1`

func (r *Registry) updateHeartbeatDB(ctx context.Context, agentID string) {
	if _, err := r.pool.Exec(ctx, updateHeartbeatSQL, agentID); err != nil {
		logger.WarningfCtx(context.Background(), "agent registry: DB heartbeat update failed for %s: %v", agentID, err)
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
type TokenRecord struct {
	AgentID   string
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

// IssueToken generates a new 24-hour token for agentID and returns it.
func (ts *TokenStore) IssueToken(agentID string) string {
	token := uuid.NewString()
	ts.mu.Lock()
	ts.tokens[token] = &TokenRecord{
		AgentID:   agentID,
		Token:     token,
		ExpiresAt: time.Now().Add(24 * time.Hour),
	}
	ts.mu.Unlock()
	return token
}

// Validate checks token validity, marks it used, and returns the bound agentID.
func (ts *TokenStore) Validate(token string) (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	rec, ok := ts.tokens[token]
	if !ok {
		return "", fmt.Errorf("bootstrap token not found")
	}
	if rec.Used {
		return "", fmt.Errorf("bootstrap token already used")
	}
	if time.Now().After(rec.ExpiresAt) {
		return "", fmt.Errorf("bootstrap token expired")
	}
	rec.Used = true
	return rec.AgentID, nil
}
