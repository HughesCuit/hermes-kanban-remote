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
	TLS        TLSConfig        `yaml:"tls"`
	Heartbeat  HeartbeatConfig  `yaml:"heartbeat"`
	Reconnect  ReconnectConfig  `yaml:"reconnect"`
	Federation FederationConfig `yaml:"federation"`
	Telemetry  telemetry.Config `yaml:"telemetry"`
}

// TLSConfig holds TLS settings for the API server.
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// HeartbeatConfig holds heartbeat timing parameters.
type HeartbeatConfig struct {
	Interval     time.Duration `yaml:"interval"`
	LeaseTimeout time.Duration `yaml:"lease_timeout"`
}

// ReconnectConfig holds auto-reconnect backoff parameters.
type ReconnectConfig struct {
	InitialInterval time.Duration `yaml:"initial_interval"`
	MaxInterval     time.Duration `yaml:"max_interval"`
	Multiplier      float64       `yaml:"multiplier"`
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
		TLS: TLSConfig{
			Enabled: false,
		},
		Heartbeat: HeartbeatConfig{
			Interval:     30 * time.Second,
			LeaseTimeout: 120 * time.Second,
		},
		Reconnect: ReconnectConfig{
			InitialInterval: 1 * time.Second,
			MaxInterval:     60 * time.Second,
			Multiplier:      2.0,
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

// ValidationError holds detailed validation information for a single field.
type ValidationError struct {
	Field      string
	Message    string
	Suggestion string
}

// Error implements the error interface.
func (ve ValidationError) Error() string {
	s := fmt.Sprintf("%s: %s", ve.Field, ve.Message)
	if ve.Suggestion != "" {
		s += fmt.Sprintf("\n  suggestion: %s", ve.Suggestion)
	}
	return s
}

// Validate checks required fields.
func (c *Config) Validate() error {
	if c.Cluster.ID == "" {
		return fmt.Errorf("cluster.id is required (set cluster.id in your config file, e.g. cluster.id: my-cluster)")
	}
	if c.Node.ID == "" {
		return fmt.Errorf("node.id is required (set node.id in your config file, e.g. node.id: my-node)")
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be 1-65535, got %d (use a valid port like 8787)", c.Server.Port)
	}
	role := c.Cluster.Role
	if role != "main" && role != "worker" {
		return fmt.Errorf("cluster.role must be 'main' or 'worker', got '%s' (set cluster.role to 'main' or 'worker')", role)
	}
	if role == "worker" && c.Cluster.Endpoint == "" {
		return fmt.Errorf("cluster.endpoint is required for worker nodes (set cluster.endpoint to the main node URL, e.g. cluster.endpoint: http://main:8787)")
	}
	if c.TLS.Enabled {
		if c.TLS.CertFile == "" {
			return fmt.Errorf("tls.cert_file is required when tls is enabled (set tls.cert_file to your certificate path)")
		}
		if c.TLS.KeyFile == "" {
			return fmt.Errorf("tls.key_file is required when tls is enabled (set tls.key_file to your private key path)")
		}
	}
	return nil
}

// ValidateDetailed checks all config fields and returns all validation errors
// with field names, messages, and suggestions. Returns nil if config is valid.
func (c *Config) ValidateDetailed() []ValidationError {
	var errs []ValidationError

	if c.Cluster.ID == "" {
		errs = append(errs, ValidationError{
			Field:      "cluster.id",
			Message:    "cluster ID is required",
			Suggestion: "set cluster.id in your config file (e.g. cluster.id: my-cluster)",
		})
	}
	if c.Node.ID == "" {
		errs = append(errs, ValidationError{
			Field:      "node.id",
			Message:    "node ID is required",
			Suggestion: "set node.id in your config file (e.g. node.id: my-node)",
		})
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		errs = append(errs, ValidationError{
			Field:      "server.port",
			Message:    fmt.Sprintf("port must be 1-65535, got %d", c.Server.Port),
			Suggestion: "use a valid port number (e.g. server.port: 8787)",
		})
	}
	role := c.Cluster.Role
	if role != "main" && role != "worker" {
		errs = append(errs, ValidationError{
			Field:      "cluster.role",
			Message:    fmt.Sprintf("must be 'main' or 'worker', got '%s'", role),
			Suggestion: "set cluster.role to 'main' for the primary node or 'worker' for secondary nodes",
		})
	}
	if role == "worker" && c.Cluster.Endpoint == "" {
		errs = append(errs, ValidationError{
			Field:      "cluster.endpoint",
			Message:    "endpoint is required for worker nodes",
			Suggestion: "set cluster.endpoint to the main node URL (e.g. cluster.endpoint: http://main:8787)",
		})
	}
	if c.TLS.Enabled {
		if c.TLS.CertFile == "" {
			errs = append(errs, ValidationError{
				Field:      "tls.cert_file",
				Message:    "certificate file is required when TLS is enabled",
				Suggestion: "set tls.cert_file to your TLS certificate path",
			})
		}
		if c.TLS.KeyFile == "" {
			errs = append(errs, ValidationError{
				Field:      "tls.key_file",
				Message:    "private key file is required when TLS is enabled",
				Suggestion: "set tls.key_file to your TLS private key path",
			})
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
