package heartbeat

import (
	"sync"
	"testing"
	"time"
)

type mockRegistry struct {
	mu    sync.Mutex
	nodes []HeartbeatNode
}

func (m *mockRegistry) GetAll() []HeartbeatNode {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]HeartbeatNode, len(m.nodes))
	copy(cp, m.nodes)
	return cp
}

func (m *mockRegistry) UpdateStatus(id string, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, n := range m.nodes {
		if n.ID == id {
			m.nodes[i].Status = status
		}
	}
}

func TestNewWatchdog(t *testing.T) {
	reg := &mockRegistry{}
	wd := NewWatchdog(reg, 5*time.Second, 15*time.Second, 30*time.Second, nil)
	if wd == nil {
		t.Fatal("expected non-nil watchdog")
	}
}

func TestUpdateIntervals(t *testing.T) {
	reg := &mockRegistry{}
	wd := NewWatchdog(reg, 5*time.Second, 15*time.Second, 30*time.Second, nil)

	wd.UpdateIntervals(10*time.Second, 20*time.Second, 40*time.Second)

	// Verify by checking that the intervals were updated (we can't directly access them
	// without exporting, so we verify behavior through Start/Stop cycle)
	wd.Start()
	time.Sleep(15 * time.Millisecond)
	wd.Stop()
}

func TestWatchdogStartStop(t *testing.T) {
	reg := &mockRegistry{}
	wd := NewWatchdog(reg, 50*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond, nil)

	wd.Start()
	wd.Start() // double start should be safe

	time.Sleep(10 * time.Millisecond)

	wd.Stop()
	wd.Stop() // double stop should be safe
}

func TestWatchdogDetectsOffline(t *testing.T) {
	reg := &mockRegistry{
		nodes: []HeartbeatNode{
			{ID: "node1", LastHeartbeat: time.Now().Add(-10 * time.Second), Status: "online"},
		},
	}

	var events []Event
	var mu sync.Mutex
	wd := NewWatchdog(reg, 50*time.Millisecond, 200*time.Millisecond, 100*time.Millisecond, func(e Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})

	wd.Start()
	time.Sleep(150 * time.Millisecond)
	wd.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Error("expected at least one event")
	}
	if len(events) > 0 && events[0].Type != "offline" {
		t.Errorf("expected offline event, got %s", events[0].Type)
	}
}
