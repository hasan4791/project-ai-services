package catalog

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/project-ai-services/ai-services/cmd/ai-services/cmd/catalog/common"
	agentdispatcher "github.com/project-ai-services/ai-services/internal/pkg/agent/dispatcher"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/gateway"
	"github.com/project-ai-services/ai-services/internal/pkg/agent/registry"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver"
	apirepository "github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver/repository"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver/services/auth"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/apiserver/services/sync"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/constants"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/db"
	"github.com/project-ai-services/ai-services/internal/pkg/catalog/db/repository"
	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	"github.com/project-ai-services/ai-services/internal/pkg/utils"
	"github.com/spf13/cobra"
)

const defaultRandomSecretKeyLength = 32

// loadDBConfig loads database configuration from environment variables.
func loadDBConfig() (db.Config, error) {
	portStr := utils.GetEnv("DB_PORT", strconv.Itoa(constants.DefaultDBPort))
	dbPort, err := strconv.Atoi(portStr)
	if err != nil {
		return db.Config{}, fmt.Errorf("invalid DB_PORT value '%s': %w", portStr, err)
	}

	dbConfig := db.Config{
		Host:     utils.GetEnv("DB_HOST", constants.DefaultDBHost),
		Port:     dbPort,
		User:     utils.GetEnv("DB_USER", constants.DefaultDBUser),
		Password: os.Getenv("DB_PASSWORD"),
		DBName:   utils.GetEnv("DB_NAME", constants.DefaultDBName),
		SSLMode:  utils.GetEnv("DB_SSLMODE", constants.DefaultSSLMode),
	}

	if dbConfig.Password == "" {
		return db.Config{}, fmt.Errorf("DB_PASSWORD environment variable is required")
	}

	return dbConfig, nil
}

// getOrGenerateSecretKey retrieves the JWT secret key from environment or generates a random one.
func getOrGenerateSecretKey() (string, error) {
	secretKey := os.Getenv("AUTH_JWT_SECRET")
	if len(secretKey) == 0 {
		logger.DebuglnCtx(context.Background(), "** WARNING: AUTH_JWT_SECRET environment variable not set. This is not recommended for production use. **")
		byteSecretKey, err := auth.GenerateRandomSecretKey(defaultRandomSecretKeyLength)
		if err != nil {
			return "", err
		}
		secretKey = string(byteSecretKey)
	}

	return secretKey, nil
}

// runAPIServer initializes and starts the API server with the provided configuration.
func runAPIServer(port int, accessTTL, refreshTTL time.Duration, adminUser, adminPassHash string, agentGatewayPort int) error {
	secretKey, err := getOrGenerateSecretKey()
	if err != nil {
		return err
	}

	dbConfig, err := loadDBConfig()
	if err != nil {
		return err
	}

	ctx := context.Background()
	pool, err := db.ConnectPool(ctx, dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	logger.Infoln("Connected to database successfully")

	userRepo := apirepository.NewInMemoryUserRepoWithAdminHash("uid_1", adminUser, "Admin", adminPassHash)
	tokenBlacklistRepo := repository.NewTokenBlacklistRepository(pool)
	blacklist := apirepository.NewDBTokenBlacklist(tokenBlacklistRepo)
	defer blacklist.Stop()

	// Initialize repositories
	applicationRepo := repository.NewApplicationRepository(pool)
	serviceRepo := repository.NewServiceRepository(pool)
	componentRepo := repository.NewComponentRepository(pool)
	serviceDependencyRepo := repository.NewServiceDependencyRepository(pool)

	// Initialize sync service for background DB-Pod synchronization
	syncService, err := sync.NewSyncService(
		applicationRepo,
		serviceRepo,
		componentRepo,
		serviceDependencyRepo,
		sync.DefaultSyncInterval,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize sync service: %w", err)
	}
	syncService.Start(ctx)
	defer syncService.Stop(ctx)

	catalogProvider, err := catalog.NewCatalogProvider()
	if err != nil {
		return fmt.Errorf("failed to initialize catalog provider: %w", err)
	}

	// Build AgentDispatcher when the AgentGateway is requested.
	// Both the dispatcher and the gateway share the same Registry instance.
	var agentDispatcher *agentdispatcher.AgentDispatcher
	opts := apiserver.APIServerOptions{
		Port:    port,
		Blacklist: blacklist,
	}
	if agentGatewayPort > 0 {
		reg := registry.New(pool)
		ts := registry.NewTokenStore()
		agentDispatcher = agentdispatcher.New(reg)
		opts.AgentGateway = gateway.New(reg, ts)
		opts.AgentGatewayPort = agentGatewayPort
		opts.AgentTokenStore = ts
		opts.AgentRegistry = reg
		logger.Infof("AgentGateway enabled on port %d", agentGatewayPort)
	}

	// Initialize application service with all required repositories.
	// agentDispatcher is nil when AgentGateway is disabled — remote-podman
	// deployments will return an error at execution time in that case.
	applicationService := apirepository.NewApplicationService(applicationRepo, serviceRepo, componentRepo, serviceDependencyRepo, catalogProvider, agentDispatcher)

	tokenMgr := auth.NewTokenManager(secretKey, accessTTL, refreshTTL)
	authSvc := auth.NewAuthService(userRepo, tokenMgr, blacklist)

	opts.AuthService = authSvc
	opts.TokenManager = tokenMgr
	opts.ApplicationService = applicationService

	return apiserver.NewAPIserver(opts).Start()
}

func NewAPIServerCmd() *cobra.Command {
	var (
		port                   = 8080
		defaultAccessTokenTTL  = time.Minute * 15
		defaultRefreshTokenTTL = time.Hour * 24 * 1
		adminUserName          string
		adminPasswordHash      string
		runtimeType            string
		agentGatewayPort       int // 0 means disabled
	)

	apiserverCmd := &cobra.Command{
		Use:   "apiserver",
		Short: "Manage AI Services API server",
		Long:  `Start the AI Services API server to provide REST endpoints for managing applications, services, and authentication.`,
		Example: `  # Start the API server with default settings
	 ai-services catalog apiserver --admin-password-hash <PASSWORD_HASH> --runtime podman

	 # Start the API server on a custom port
	 ai-services catalog apiserver --port 9090 --admin-password-hash <PASSWORD_HASH> --runtime podman

	 # Start with AgentGateway enabled (for remote worker agents)
	 ai-services catalog apiserver --admin-password-hash <PASSWORD_HASH> --runtime podman --agentgateway-port 9090

Note:
  - Requires database connection via environment variables (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
  - AUTH_JWT_SECRET environment variable is recommended for production use`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return common.InitAndValidateRuntimeFlag(runtimeType)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAPIServer(port, defaultAccessTokenTTL, defaultRefreshTokenTTL, adminUserName, adminPasswordHash, agentGatewayPort)
		},
	}

	apiserverCmd.Flags().IntVarP(&port, "port", "p", port, "Port for the API server to listen on")
	apiserverCmd.Flags().DurationVarP(&defaultAccessTokenTTL, "access-token-ttl", "", defaultAccessTokenTTL, "Time-to-live for access tokens")
	apiserverCmd.Flags().DurationVarP(&defaultRefreshTokenTTL, "refresh-token-ttl", "", defaultRefreshTokenTTL, "Time-to-live for refresh tokens")
	apiserverCmd.Flags().StringVar(&adminUserName, "admin-username", "admin", "Username for the default admin user")
	apiserverCmd.Flags().StringVar(&adminPasswordHash, "admin-password-hash", "", "Precomputed hash of the password for the default admin user")
	apiserverCmd.Flags().IntVar(&agentGatewayPort, "agentgateway-port", 0, "Port for the gRPC AgentGateway (0 = disabled, default 9090 when enabled)")
	common.ConfigureRuntimeFlag(apiserverCmd, &runtimeType)

	return apiserverCmd
}
