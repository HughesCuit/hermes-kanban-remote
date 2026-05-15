package telemetry

import (
	"context"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Enabled {
		t.Error("expected Enabled=false by default")
	}
	if cfg.Exporter != "none" {
		t.Errorf("expected Exporter='none', got %q", cfg.Exporter)
	}
	if cfg.Endpoint != "localhost:4317" {
		t.Errorf("expected Endpoint='localhost:4317', got %q", cfg.Endpoint)
	}
	if cfg.ServiceName != "hermes-agent-cluster" {
		t.Errorf("expected ServiceName='hermes-agent-cluster', got %q", cfg.ServiceName)
	}
	if cfg.SampleRate != 1.0 {
		t.Errorf("expected SampleRate=1.0, got %f", cfg.SampleRate)
	}
	if cfg.BatchTimeout != 5*time.Second {
		t.Errorf("expected BatchTimeout=5s, got %v", cfg.BatchTimeout)
	}
}

func TestConfigIsEnabled(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected bool
	}{
		{
			name:     "disabled by default",
			config:   DefaultConfig(),
			expected: false,
		},
		{
			name:     "enabled but exporter=none",
			config:   Config{Enabled: true, Exporter: "none"},
			expected: false,
		},
		{
			name:     "disabled with otlp exporter",
			config:   Config{Enabled: false, Exporter: "otlp"},
			expected: false,
		},
		{
			name:     "enabled with otlp exporter",
			config:   Config{Enabled: true, Exporter: "otlp"},
			expected: true,
		},
		{
			name:     "enabled with stdout exporter",
			config:   Config{Enabled: true, Exporter: "stdout"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.config.IsEnabled(); got != tt.expected {
				t.Errorf("IsEnabled() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestProviderInitDisabled(t *testing.T) {
	cfg := DefaultConfig() // disabled by default
	provider, err := Init(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Init() with disabled config returned error: %v", err)
	}
	if provider == nil {
		t.Fatal("Init() returned nil provider")
	}
	if provider.TracerProvider != nil {
		t.Error("expected nil TracerProvider when disabled")
	}
	// Shutdown should not error
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() returned error: %v", err)
	}
}

func TestProviderInitUnsupportedExporter(t *testing.T) {
	cfg := Config{
		Enabled:  true,
		Exporter: "unsupported",
	}
	_, err := Init(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unsupported exporter")
	}
}

func TestNewMetrics(t *testing.T) {
	m, err := NewMetrics()
	if err != nil {
		t.Fatalf("NewMetrics() returned error: %v", err)
	}
	if m == nil {
		t.Fatal("NewMetrics() returned nil")
	}

	// Verify all metrics are non-nil
	if m.TasksCreated == nil {
		t.Error("TasksCreated is nil")
	}
	if m.TasksCompleted == nil {
		t.Error("TasksCompleted is nil")
	}
	if m.TasksFailed == nil {
		t.Error("TasksFailed is nil")
	}
	if m.TasksScheduled == nil {
		t.Error("TasksScheduled is nil")
	}
	if m.LeasesCreated == nil {
		t.Error("LeasesCreated is nil")
	}
	if m.LeasesExpired == nil {
		t.Error("LeasesExpired is nil")
	}
	if m.LeasesRevoked == nil {
		t.Error("LeasesRevoked is nil")
	}
	if m.NodesOnline == nil {
		t.Error("NodesOnline is nil")
	}
	if m.RecoveryEvents == nil {
		t.Error("RecoveryEvents is nil")
	}
	if m.SyncMessagesReceived == nil {
		t.Error("SyncMessagesReceived is nil")
	}
	if m.SchedulingDuration == nil {
		t.Error("SchedulingDuration is nil")
	}
	if m.HTTPRequestDuration == nil {
		t.Error("HTTPRequestDuration is nil")
	}
}

func TestTracerReturnsNoop(t *testing.T) {
	// Without setting a provider, Tracer should return a noop tracer
	tracer := Tracer("test")
	if tracer == nil {
		t.Error("Tracer() returned nil")
	}
	// The noop tracer won't panic on use
	_, span := tracer.Start(context.Background(), "test-span")
	span.End()
}
