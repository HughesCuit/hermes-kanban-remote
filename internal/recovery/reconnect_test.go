package recovery

import (
	"fmt"
	"testing"
	"time"
)

func TestNewReconnectManager(t *testing.T) {
	cfg := ReconnectConfig{
		InitialInterval: 1 * time.Second,
		MaxInterval:     60 * time.Second,
		Multiplier:      2.0,
	}
	rm := NewReconnectManager(cfg, func(string) error { return nil })
	if rm == nil {
		t.Fatal("expected non-nil manager")
	}
}

func TestNotifyDisconnect_Connect(t *testing.T) {
	cfg := ReconnectConfig{InitialInterval: time.Second, MaxInterval: time.Minute, Multiplier: 2.0}
	rm := NewReconnectManager(cfg, func(string) error { return nil })

	rm.NotifyDisconnect("target1")
	state := rm.GetState("target1")
	if state == nil {
		t.Fatal("expected state for target1")
	}
	if state.Connected {
		t.Error("expected disconnected")
	}
	if state.CurrentInterval != time.Second {
		t.Errorf("expected initial interval 1s, got %v", state.CurrentInterval)
	}

	rm.NotifyConnect("target1")
	state = rm.GetState("target1")
	if !state.Connected {
		t.Error("expected connected")
	}
}

func TestExponentialBackoff(t *testing.T) {
	cfg := ReconnectConfig{
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
	}

	rm := NewReconnectManager(cfg, func(target string) error {
		return fmt.Errorf("connection refused")
	})

	rm.NotifyDisconnect("target1")

	// Each call has a very small interval so it triggers immediately
	rm.checkDisconnected()
	state := rm.GetState("target1")
	if state == nil {
		t.Fatal("expected state")
	}
	if state.ConsecutiveFails != 1 {
		t.Errorf("expected 1 consecutive fail, got %d", state.ConsecutiveFails)
	}
	if state.CurrentInterval != 2*time.Millisecond {
		t.Errorf("expected interval 2ms after first fail, got %v", state.CurrentInterval)
	}

	// Wait for the new interval to elapse
	time.Sleep(3 * time.Millisecond)
	rm.checkDisconnected()
	state = rm.GetState("target1")
	if state.ConsecutiveFails != 2 {
		t.Errorf("expected 2 consecutive fails, got %d", state.ConsecutiveFails)
	}
	if state.CurrentInterval != 4*time.Millisecond {
		t.Errorf("expected interval 4ms after second fail, got %v", state.CurrentInterval)
	}

	time.Sleep(5 * time.Millisecond)
	rm.checkDisconnected()
	state = rm.GetState("target1")
	if state.ConsecutiveFails != 3 {
		t.Errorf("expected 3 consecutive fails, got %d", state.ConsecutiveFails)
	}
	if state.CurrentInterval != 8*time.Millisecond {
		t.Errorf("expected interval 8ms after third fail, got %v", state.CurrentInterval)
	}
}

func TestBackoffMaxInterval(t *testing.T) {
	cfg := ReconnectConfig{
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     8 * time.Millisecond,
		Multiplier:      2.0,
	}

	rm := NewReconnectManager(cfg, func(target string) error {
		return fmt.Errorf("refused")
	})

	rm.NotifyDisconnect("target1")

	// Simulate enough failures to exceed max
	for i := 0; i < 10; i++ {
		rm.checkDisconnected()
		time.Sleep(2 * time.Millisecond)
	}
	state := rm.GetState("target1")
	if state.CurrentInterval > cfg.MaxInterval {
		t.Errorf("interval %v exceeds max %v", state.CurrentInterval, cfg.MaxInterval)
	}
}

func TestReconnectResetsOnConnect(t *testing.T) {
	cfg := ReconnectConfig{
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
	}

	rm := NewReconnectManager(cfg, func(target string) error {
		return fmt.Errorf("refused")
	})

	rm.NotifyDisconnect("target1")
	rm.checkDisconnected()
	time.Sleep(2 * time.Millisecond)
	rm.checkDisconnected()

	rm.NotifyConnect("target1")
	state := rm.GetState("target1")
	if !state.Connected {
		t.Error("expected connected after NotifyConnect")
	}
	if state.ConsecutiveFails != 0 {
		t.Errorf("expected 0 consecutive fails after reconnect, got %d", state.ConsecutiveFails)
	}
	if state.CurrentInterval != cfg.InitialInterval {
		t.Errorf("expected interval reset to %v, got %v", cfg.InitialInterval, state.CurrentInterval)
	}
}

func TestGetState_NonexistentTarget(t *testing.T) {
	cfg := ReconnectConfig{InitialInterval: time.Second, MaxInterval: time.Minute, Multiplier: 2.0}
	rm := NewReconnectManager(cfg, func(string) error { return nil })
	if rm.GetState("nonexistent") != nil {
		t.Error("expected nil for nonexistent target")
	}
}

func TestStartStop(t *testing.T) {
	cfg := ReconnectConfig{InitialInterval: time.Second, MaxInterval: time.Minute, Multiplier: 2.0}
	rm := NewReconnectManager(cfg, func(string) error { return nil })

	rm.Start()
	rm.Start() // double start should be safe
	rm.Stop()
	rm.Stop() // double stop should be safe
}
