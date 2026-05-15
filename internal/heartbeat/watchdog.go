package heartbeat

import (
	"sync"
	"time"
)

// Event types emitted by the watchdog.
type Event struct {
	NodeID string
	Type   string // "online", "degraded", "offline"
}

// Watchdog monitors node heartbeats and emits status change events.
type Watchdog struct {
	mu               sync.Mutex
	interval         time.Duration
	degradedAfter    time.Duration
	offlineAfter     time.Duration
	callback         func(Event)
	running          bool
	stopCh           chan struct{}
	registry         HeartbeatRegistry
	intervalUpdated  bool // flag set when interval changes while running
}

// HeartbeatRegistry is the interface the watchdog needs from the cluster registry.
type HeartbeatRegistry interface {
	GetAll() []HeartbeatNode
	UpdateStatus(id string, status string)
}

// HeartbeatNode is a minimal node interface for the watchdog.
type HeartbeatNode struct {
	ID            string
	LastHeartbeat time.Time
	Status        string
}

// NewWatchdog creates a watchdog that checks heartbeat status.
// checkInterval: how often to check
// degradedAfter: time without heartbeat to mark degraded
// offlineAfter: time without heartbeat to mark offline
func NewWatchdog(reg HeartbeatRegistry, checkInterval, degradedAfter, offlineAfter time.Duration, callback func(Event)) *Watchdog {
	return &Watchdog{
		interval:      checkInterval,
		degradedAfter: degradedAfter,
		offlineAfter:  offlineAfter,
		callback:      callback,
		registry:      reg,
	}
}

// Start begins the watchdog loop.
func (w *Watchdog) Start() {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.stopCh = make(chan struct{})
	w.mu.Unlock()

	go func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.check()
				// Check if interval was updated; recreate ticker if so
				w.mu.Lock()
				if w.intervalUpdated {
					w.intervalUpdated = false
					interval := w.interval
					w.mu.Unlock()
					ticker.Stop()
					ticker = time.NewTicker(interval)
				} else {
					w.mu.Unlock()
				}
			case <-w.stopCh:
				return
			}
		}
	}()
}

// Stop halts the watchdog.
func (w *Watchdog) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.running {
		return
	}
	w.running = false
	close(w.stopCh)
}

// UpdateIntervals dynamically changes the watchdog timing parameters.
func (w *Watchdog) UpdateIntervals(checkInterval, degradedAfter, offlineAfter time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		w.intervalUpdated = true
	}
	w.interval = checkInterval
	w.degradedAfter = degradedAfter
	w.offlineAfter = offlineAfter
}

func (w *Watchdog) check() {
	now := time.Now()
	nodes := w.registry.GetAll()
	for _, n := range nodes {
		elapsed := now.Sub(n.LastHeartbeat)
		var newStatus string
		switch {
		case elapsed >= w.offlineAfter:
			newStatus = "offline"
		case elapsed >= w.degradedAfter:
			newStatus = "degraded"
		default:
			newStatus = "online"
		}

		if newStatus != n.Status {
			w.registry.UpdateStatus(n.ID, newStatus)
			if w.callback != nil {
				w.callback(Event{NodeID: n.ID, Type: newStatus})
			}
		}
	}
}
