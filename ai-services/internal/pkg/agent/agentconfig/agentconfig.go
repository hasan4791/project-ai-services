// Package agentconfig manages the local agent configuration stored at
// ~/.config/ai-services/agent.json. It persists the agent name written by
// `ai-services agent start` so that subsequent commands (e.g. `agent status`)
// do not require --name to be re-specified.
package agentconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	configDirName  = "ai-services"
	configFileName = "agent.json"
)

// ErrNoAgentConfig is returned when no agent config file exists.
var ErrNoAgentConfig = errors.New("no agent config found: run 'ai-services agent start' first")

// AgentConfig holds the locally persisted agent settings.
type AgentConfig struct {
	// AgentName is the name this agent registered under.
	AgentName string `json:"agent_name"`
	// Server is the control-plane AgentGateway address (host:port).
	Server string `json:"server"`
	// DomainSuffix is the computed domain suffix for route registration,
	// saved by 'agent configure' and sent to the control plane at start-up.
	DomainSuffix string `json:"domain_suffix,omitempty"`
}

func configFilePath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine user config directory: %w", err)
	}
	return filepath.Join(base, configDirName, configFileName), nil
}

// Save persists the agent config to ~/.config/ai-services/agent.json.
func Save(cfg AgentConfig) error {
	const (
		dirPerm  = 0o700
		filePerm = 0o600
	)

	path, err := configFilePath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), dirPerm); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal agent config: %w", err)
	}

	if err := os.WriteFile(path, data, filePerm); err != nil {
		return fmt.Errorf("write agent config: %w", err)
	}

	return nil
}

// Load reads the agent config from disk.
// Returns ErrNoAgentConfig if the file does not exist.
func Load() (AgentConfig, error) {
	path, err := configFilePath()
	if err != nil {
		return AgentConfig{}, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return AgentConfig{}, ErrNoAgentConfig
		}
		return AgentConfig{}, fmt.Errorf("read agent config: %w", err)
	}

	var cfg AgentConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return AgentConfig{}, fmt.Errorf("parse agent config: %w", err)
	}

	return cfg, nil
}
