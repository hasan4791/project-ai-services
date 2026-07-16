// Package apiserver provides the implementation of the API server for the AI Services Catalog.
// It includes the setup of routes, authentication, and server configuration.
//
//	@title						AI Services Catalog API
//	@version					1.0
//	@description				API server for managing AI Services catalog, applications, and authentication
//	@termsOfService				http://swagger.io/terms/
//
//	@contact.name				API Support
//	@contact.url				https://github.com/project-ai-services/ai-services
//	@contact.email				support@example.com
//
//	@license.name				Apache 2.0
//	@license.url				http://www.apache.org/licenses/LICENSE-2.0.html
//
//	@host						localhost:8080
//	@BasePath					/api/v1
//
//	@tag.name					Authentication
//	@tag.description			Authentication and authorization endpoints
//
//	@tag.name					Applications
//	@tag.description			Application management endpoints
//
//	@tag.name					Catalog
//	@tag.description			Catalog endpoints for architectures and services
//
//	@securityDefinitions.apikey	BearerAuth
//	@in							header
//	@name						Authorization
//	@description				Type "Bearer" followed by a space and JWT token.
package apiserver

import (
	"context"
	"fmt"

	"github.com/project-ai-services/ai-services/internal/pkg/agent/gateway"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/registry"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver/repository"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver/services/auth"
)

// APIServerOptions defines the configuration options for the API server such as the port to listen
// on and the authentication provider.
type APIServerOptions struct {
	Port               int
	AuthService        auth.Service
	TokenManager       *auth.TokenManager
	Blacklist          repository.TokenBlacklist
	ApplicationService *repository.ApplicationService
	// AgentGateway is optional. When non-nil the gRPC AgentGateway is started
	// alongside the REST server on AgentGatewayPort.
	AgentGateway     *gateway.Gateway
	AgentGatewayPort int // defaults to 9090 when AgentGateway is set
	// AgentTokenStore and AgentRegistry are passed through to the REST handler
	// so admins can issue bootstrap tokens and list agents via the REST API.
	AgentTokenStore *registry.TokenStore
	AgentRegistry   *registry.Registry
}

// APIserver represents the API server instance, holding the configuration and authentication provider.
type APIserver struct {
	port               int
	authService        auth.Service
	tokenManager       *auth.TokenManager
	blacklist          repository.TokenBlacklist
	applicationService *repository.ApplicationService
	agentGateway       *gateway.Gateway
	agentGatewayPort   int
	agentTokenStore    *registry.TokenStore
	agentRegistry      *registry.Registry
}

// NewAPIserver creates a new instance of the API server with the provided options, setting default values where necessary.
func NewAPIserver(options APIServerOptions) *APIserver {
	if options.Port == 0 {
		options.Port = 8080
	}
	if options.AgentGateway != nil && options.AgentGatewayPort == 0 {
		options.AgentGatewayPort = 9090
	}

	return &APIserver{
		port:               options.Port,
		authService:        options.AuthService,
		tokenManager:       options.TokenManager,
		blacklist:          options.Blacklist,
		applicationService: options.ApplicationService,
		agentGateway:       options.AgentGateway,
		agentGatewayPort:   options.AgentGatewayPort,
		agentTokenStore:    options.AgentTokenStore,
		agentRegistry:      options.AgentRegistry,
	}
}

// Start initializes the API server and begins listening for incoming requests on the configured port.
// If an AgentGateway is configured it is started on AgentGatewayPort before the REST server.
func (a *APIserver) Start() error {
	if a.agentGateway != nil {
		ctx := context.Background()
		addr := fmt.Sprintf(":%d", a.agentGatewayPort)
		if err := a.agentGateway.Start(ctx, addr); err != nil {
			return fmt.Errorf("failed to start AgentGateway: %w", err)
		}
	}

	r := CreateRouter(a.authService, a.tokenManager, a.blacklist, a.applicationService, a.agentTokenStore, a.agentRegistry)
	return r.Run(fmt.Sprintf(":%d", a.port))
}
