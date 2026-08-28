// Package constants holds constants shared across the worker sub-packages
// (deploy, join, uninstall, gateway, etc.) to avoid duplication.
package constants

const (
	// WorkerProxyLabel is the pod label set by deploy.Setup; used by deploy (idempotency) and uninstall (lookup).
	WorkerProxyLabel = "ai-services.io/component=worker-proxy"

	// WorkerDataSubDir is the on-disk subtree written by deploy.Setup; removed by uninstall.
	WorkerDataSubDir = "worker"

	// BaseDirEnvVar is injected into the Caddy container at deploy time; read back by uninstall.
	BaseDirEnvVar = "AI_SERVICES_BASE_DIR"

	// MetaKeyBaseDir, MetaKeyDomainSuffix, MetaKeyHTTPSPort are RegisterRequest.Metadata keys sent during join.
	MetaKeyBaseDir      = "basedir"
	MetaKeyDomainSuffix = "domainSuffix"
	MetaKeyHTTPSPort    = "httpsPort"

	// GatewayServerName is the fixed DNS SAN embedded in the auto-generated gateway server
	// certificate. Workers set tls.Config.ServerName to this value so hostname verification
	// succeeds regardless of the IP or public DNS name used to reach the gateway.
	// Must match gateway.GatewayServerName — defined here to avoid an import cycle.
	GatewayServerName = "worker-gateway.ai-services.internal"
)
