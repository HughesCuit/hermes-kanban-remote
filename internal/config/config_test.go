package config

import (
	"testing"
	"time"
)

func TestDefaultConfig_TLSDisabled(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.TLS.Enabled {
		t.Error("expected TLS disabled by default")
	}
}

func TestValidate_TLSEnabled_MissingCertFile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLS.Enabled = true
	cfg.TLS.KeyFile = "key.pem"
	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for missing cert_file")
	}
}

func TestValidate_TLSEnabled_MissingKeyFile(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = "cert.pem"
	err := cfg.Validate()
	if err == nil {
		t.Error("expected validation error for missing key_file")
	}
}

func TestValidate_TLSEnabled_BothSet(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = "cert.pem"
	cfg.TLS.KeyFile = "key.pem"
	err := cfg.Validate()
	if err != nil {
		t.Errorf("unexpected validation error: %v", err)
	}
}

func TestValidate_TLSDisabled_IgnoresCertKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLS.Enabled = false
	// cert and key empty should be fine when TLS disabled
	err := cfg.Validate()
	if err != nil {
		t.Errorf("unexpected validation error: %v", err)
	}
}

func TestDefaultConfig_HeartbeatValues(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Heartbeat.Interval != 30*time.Second {
		t.Errorf("expected heartbeat interval 30s, got %v", cfg.Heartbeat.Interval)
	}
	if cfg.Heartbeat.LeaseTimeout != 120*time.Second {
		t.Errorf("expected heartbeat lease timeout 120s, got %v", cfg.Heartbeat.LeaseTimeout)
	}
}

func TestDefaultConfig_ReconnectValues(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Reconnect.InitialInterval != 1*time.Second {
		t.Errorf("expected reconnect initial 1s, got %v", cfg.Reconnect.InitialInterval)
	}
	if cfg.Reconnect.MaxInterval != 60*time.Second {
		t.Errorf("expected reconnect max 60s, got %v", cfg.Reconnect.MaxInterval)
	}
	if cfg.Reconnect.Multiplier != 2.0 {
		t.Errorf("expected reconnect multiplier 2.0, got %v", cfg.Reconnect.Multiplier)
	}
}
