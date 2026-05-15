package config

import (
	"fmt"
	"os"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/telemetry"
	"gopkg.in/yaml.v3"
)

// Config represents the full cluster configuration.
type Config struct {
	Cluster    ClusterConfig    `yaml:"cluster"`
	Node       NodeConfig       `yaml:"node"`
	Server     ServerConfig     `yaml:"server"`
	Lease      LeaseConfig      `yaml:"lease"`
	Watchdog   WatchdogConfig   `yaml:"watchdog"`
	Federation FederationConfig `yaml:"federation"`
	Telemetry  telemetry.Config  `yaml:"telemetry"`
}

// ClusterConfig holds cluster-wide settings.
type ClusterConfig struct {
	ID       string `yaml:"id"`
	Role     string `yaml:"role"`     // "main" or "worker"
	Endpoint string `yaml:"endpoint"` // main node URL (worker only)
	Token    string `yaml:"token"`    // cluster auth token
}

// NodeConfig holds this node's identity and capabilities.
type NodeConfig struct {
	ID           string   `yaml:"id"`
	Name         string   `yaml:"name"`
	Capabilities []string `yaml:"capabilities"`
}

// ServerConfig holds the HTTP server settings.
type ServerConfig struct {
	Bind string `yaml:"bind"` // e.g. "0.0.0.0"
	Port int    `yaml:"port"`
}

// LeaseConfig holds lease timing parameters.
type LeaseConfig struct {
	TTL      time.Duration `yaml:"ttl"`
	ScanRate time.Duration `yaml:"scan_rate"`
}

// WatchdogConfig holds heartbeat watchdog parameters.
type WatchdogConfig struct {
	CheckInterval time.Duration `yaml:"check_interval"`
	DegradedAfter  time.Duration `yaml:"degraded_after"`
	OfflineAfter   time.Duration `yaml:"offline_after"`
}

// FederationConfig holds cross-cluster federation settings.
type FederationConfig struct {
	Enabled       bool          `yaml:"enabled"`
	PingInterval  time.Duration `yaml:"ping_interval"`
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig() *Config {
	return &Config{
		Cluster: ClusterConfig{
			ID:   "cluster_default",
			Role: "main",
		},
		Node: NodeConfig{
			ID:   "node_main",
			Name: "main-node",
		},
		Server: ServerConfig{
			Bind: "0.0.0.0",
			Port: 8787,
		},
		Lease: LeaseConfig{
			TTL:      60 * time.Second,
			ScanRate: 10 * time.Second,
		},
		Watchdog: WatchdogConfig{
			CheckInterval: 5 * time.Second,
			DegradedAfter:  15 * time.Second,
			OfflineAfter:   30 * time.Second,
		},
		Federation: FederationConfig{
			Enabled:      true,
			PingInterval: 30 * time.Second,
		},
		Telemetry: telemetry.DefaultConfig(),
	}
}

// Load reads configuration from a YAML file.
// Falls back to defaults for missing fields.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // use defaults
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return cfg, nil
}

// Save writes the config to a YAML file.
func Save(cfg *Config, path string) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

// BindAddress returns the full "host:port" string.
func (c *ServerConfig) BindAddress() string {
	return fmt.Sprintf("%s:%d", c.Bind, c.Port)
}

// Validate checks required fields.
func (c *Config) Validate() error {
	if c.Cluster.ID == "" {
		return fmt.Errorf("cluster.id is required")
	}
	if c.Node.ID == "" {
		return fmt.Errorf("node.id is required")
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be 1-65535, got %d", c.Server.Port)
	}
	role := c.Cluster.Role
	if role != "main" && role != "worker" {
		return fmt.Errorf("cluster.role must be 'main' or 'worker', got '%s'", role)
	}
	if role == "worker" && c.Cluster.Endpoint == "" {
		return fmt.Errorf("cluster.endpoint is required for worker nodes")
	}
	return nil
}
