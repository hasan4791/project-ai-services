package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/registry"
)

// AgentHandler handles agent management HTTP requests.
// tokenStore and reg may both be nil when the AgentGateway is disabled.
type AgentHandler struct {
	tokenStore *registry.TokenStore
	reg        *registry.Registry
}

// NewAgentHandler creates a new AgentHandler.
func NewAgentHandler(tokenStore *registry.TokenStore, reg *registry.Registry) *AgentHandler {
	return &AgentHandler{tokenStore: tokenStore, reg: reg}
}

// IssueToken godoc
//
//	@Summary		Issue a bootstrap token for a worker agent
//	@Description	Generates a single-use 24-hour HMAC bootstrap token for the given agent_id.
//	@Description	The admin copies this token to /etc/ai-services/agent.conf on the Worker LPAR
//	@Description	before running `ai-services bootstrap configure --runtime podman --agent`.
//	@Tags			Agents
//	@Accept			json
//	@Produce		json
//	@Security		BearerAuth
//	@Param			request	body		IssueTokenRequest	true	"Agent ID to issue a token for"
//	@Success		201		{object}	IssueTokenResponse
//	@Failure		400		{object}	ErrorResponse	"Missing or invalid agent_id"
//	@Failure		401		{object}	ErrorResponse	"Unauthorized"
//	@Failure		501		{object}	ErrorResponse	"AgentGateway not enabled on this server"
//	@Router			/agents/tokens [post]
func (h *AgentHandler) IssueToken(c *gin.Context) {
	if h.tokenStore == nil {
		c.JSON(http.StatusNotImplemented, ErrorResponse{
			Error: "AgentGateway is not enabled on this server (start with --agentgateway-port)",
		})
		return
	}

	var req IssueTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: "invalid request body: " + err.Error(),
		})
		return
	}

	token := h.tokenStore.IssueToken(req.AgentID)

	c.JSON(http.StatusCreated, IssueTokenResponse{
		AgentID: req.AgentID,
		Token:   token,
		Note:    "Write this token to /etc/ai-services/agent.conf on the Worker LPAR as pre_shared_token. It expires in 24 h and is single-use.",
	})
}

// GetAgent godoc
//
//	@Summary		Get a registered worker agent by ID
//	@Description	Returns the live registry status for a single agent.
//	@Tags			Agents
//	@Produce		json
//	@Security		BearerAuth
//	@Param			agent_id	path		string	true	"Agent ID"
//	@Success		200			{object}	AgentInfo
//	@Failure		404			{object}	ErrorResponse	"Agent not found"
//	@Failure		401			{object}	ErrorResponse	"Unauthorized"
//	@Failure		501			{object}	ErrorResponse	"AgentGateway not enabled on this server"
//	@Router			/agents/{agent_id} [get]
func (h *AgentHandler) GetAgent(c *gin.Context) {
	if h.reg == nil {
		c.JSON(http.StatusNotImplemented, ErrorResponse{
			Error: "AgentGateway is not enabled on this server (start with --agentgateway-port)",
		})
		return
	}

	agentID := c.Param("agent_id")
	snap := h.reg.Snapshot()
	for _, s := range snap {
		if s.AgentID == agentID {
			ai := AgentInfo{
				AgentID: s.AgentID,
				Status:  string(s.Status),
				Labels:  s.Labels,
				ActiveSlots: s.ActiveSlots,
			}
			if !s.LastHeartbeat.IsZero() {
				ai.LastHeartbeat = s.LastHeartbeat.UTC().Format(time.RFC3339)
			}
			c.JSON(http.StatusOK, ai)
			return
		}
	}
	c.JSON(http.StatusNotFound, ErrorResponse{Error: "agent " + agentID + " not found"})
}

// DeleteAgent godoc
//
//	@Summary		Delete a registered worker agent
//	@Description	Removes the agent from the in-memory registry and the database.
//	@Description	If the agent has an active CommandStream it will be disconnected on its next send/recv.
//	@Tags			Agents
//	@Produce		json
//	@Security		BearerAuth
//	@Param			agent_id	path		string	true	"Agent ID to delete"
//	@Success		200			{object}	map[string]string
//	@Failure		404			{object}	ErrorResponse	"Agent not found"
//	@Failure		401			{object}	ErrorResponse	"Unauthorized"
//	@Failure		501			{object}	ErrorResponse	"AgentGateway not enabled on this server"
//	@Router			/agents/{agent_id} [delete]
func (h *AgentHandler) DeleteAgent(c *gin.Context) {
	if h.reg == nil {
		c.JSON(http.StatusNotImplemented, ErrorResponse{
			Error: "AgentGateway is not enabled on this server (start with --agentgateway-port)",
		})
		return
	}

	agentID := c.Param("agent_id")
	if agentID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "agent_id path parameter is required"})
		return
	}

	if err := h.reg.Delete(c.Request.Context(), agentID); err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"deleted": agentID})
}

// ListAgents godoc
//
//	@Summary		List registered worker agents
//	@Description	Returns a snapshot of all agents known to the AgentGateway registry.
//	@Tags			Agents
//	@Produce		json
//	@Security		BearerAuth
//	@Success		200	{object}	ListAgentsResponse
//	@Failure		401	{object}	ErrorResponse	"Unauthorized"
//	@Failure		501	{object}	ErrorResponse	"AgentGateway not enabled on this server"
//	@Router			/agents [get]
func (h *AgentHandler) ListAgents(c *gin.Context) {
	if h.reg == nil {
		c.JSON(http.StatusNotImplemented, ErrorResponse{
			Error: "AgentGateway is not enabled on this server (start with --agentgateway-port)",
		})
		return
	}

	snap := h.reg.Snapshot()
	agents := make([]AgentInfo, 0, len(snap))
	for _, s := range snap {
		ai := AgentInfo{
			AgentID: s.AgentID,
			Status:  string(s.Status),
			Labels:  s.Labels,
		}
		if !s.LastHeartbeat.IsZero() {
			ai.LastHeartbeat = s.LastHeartbeat.UTC().Format(time.RFC3339)
		}
		agents = append(agents, ai)
	}

	c.JSON(http.StatusOK, ListAgentsResponse{Agents: agents})
}

// ──────────────────────────────────────────────────────────────────────────────
// Request / response models
// ──────────────────────────────────────────────────────────────────────────────

// IssueTokenRequest is the body for POST /api/v1/agents/tokens.
type IssueTokenRequest struct {
	AgentID string `json:"agent_id" binding:"required"`
}

// IssueTokenResponse is returned by POST /api/v1/agents/tokens.
type IssueTokenResponse struct {
	AgentID string `json:"agent_id"`
	Token   string `json:"token"`
	Note    string `json:"note"`
}

// AgentInfo is a single row in the ListAgents response.
type AgentInfo struct {
	AgentID       string            `json:"agent_id"`
	Status        string            `json:"status"`
	Labels        map[string]string `json:"labels"`
	LastHeartbeat string            `json:"last_heartbeat,omitempty"`
	ActiveSlots   int               `json:"active_slots"`
}

// ListAgentsResponse is returned by GET /api/v1/agents.
type ListAgentsResponse struct {
	Agents []AgentInfo `json:"agents"`
}
