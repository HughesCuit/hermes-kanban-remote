package telemetry

import "time"

// Config holds OpenTelemetry configuration.
type Config struct {
	Enabled      bool          `yaml:"enabled"`
	Exporter     string        `yaml:"exporter"`      // "otlp", "stdout", "none"
	Endpoint     string        `yaml:"endpoint"`       // OTLP gRPC endpoint (e.g. "localhost:4317")
	ServiceName  string        `yaml:"service_name"`
	SampleRate   float64       `yaml:"sample_rate"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:      false,
		Exporter:     "none",
		Endpoint:     "localhost:4317",
		ServiceName:  "hermes-agent-cluster",
		SampleRate:   1.0,
		BatchTimeout: 5 * time.Second,
	}
}

// IsEnabled returns true if telemetry should be active.
func (c Config) IsEnabled() bool {
	return c.Enabled && c.Exporter != "none"
}
