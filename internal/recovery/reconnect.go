package recovery

import (
	"sync"
	"time"
)

// ReconnectConfig holds backoff parameters for reconnection.
type ReconnectConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

// ReconnectState tracks the reconnect status for a single target.
type ReconnectState struct {
	Target           string
	CurrentInterval  time.Duration
	LastAttempt      time.Time
	ConsecutiveFails int
	Connected        bool
}

// ReconnectManager manages automatic reconnection with exponential backoff.
type ReconnectManager struct {
	config    ReconnectConfig
	targets   map[string]*ReconnectState
	callback  func(target string) error
	mu        sync.RWMutex
	stopCh    chan struct{}
	running   bool
}

// NewReconnectManager creates a new reconnect manager.
func NewReconnectManager(cfg ReconnectConfig, callback func(string) error) *ReconnectManager {
	return &ReconnectManager{
		config:   cfg,
		targets:  make(map[string]*ReconnectState),
		callback: callback,
	}
}

// Start begins the reconnection loop.
func (rm *ReconnectManager) Start() {
	rm.mu.Lock()
	if rm.running {
		rm.mu.Unlock()
		return
	}
	rm.running = true
	rm.stopCh = make(chan struct{})
	stopCh := rm.stopCh
	rm.mu.Unlock()

	go rm.loop(stopCh)
}

// Stop halts the reconnection loop.
func (rm *ReconnectManager) Stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if !rm.running {
		return
	}
	rm.running = false
	close(rm.stopCh)
}

// NotifyDisconnect marks a target as disconnected and triggers reconnection attempts.
func (rm *ReconnectManager) NotifyDisconnect(target string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rs, ok := rm.targets[target]
	if !ok {
		rs = &ReconnectState{
			Target:          target,
			CurrentInterval: rm.config.InitialInterval,
		}
		rm.targets[target] = rs
	}
	rs.Connected = false
}

// NotifyConnect marks a target as connected and resets its backoff interval.
func (rm *ReconnectManager) NotifyConnect(target string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rs, ok := rm.targets[target]
	if !ok {
		rs = &ReconnectState{Target: target}
		rm.targets[target] = rs
	}
	rs.Connected = true
	rs.ConsecutiveFails = 0
	rs.CurrentInterval = rm.config.InitialInterval
}

// GetState returns a copy of the reconnect state for a target.
func (rm *ReconnectManager) GetState(target string) *ReconnectState {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	rs, ok := rm.targets[target]
	if !ok {
		return nil
	}
	cp := *rs
	return &cp
}

func (rm *ReconnectManager) loop(stopCh chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.checkDisconnected()
		case <-stopCh:
			return
		}
	}
}

func (rm *ReconnectManager) checkDisconnected() {
	rm.mu.Lock()
	type reconnectTask struct {
		target string
		state  *ReconnectState
	}
	var tasks []reconnectTask
	now := time.Now()

	for _, rs := range rm.targets {
		if rs.Connected {
			continue
		}
		if now.Sub(rs.LastAttempt) >= rs.CurrentInterval {
			tasks = append(tasks, reconnectTask{
				target: rs.Target,
				state:  rs,
			})
			rs.LastAttempt = now
		}
	}
	rm.mu.Unlock()

	for _, task := range tasks {
		err := rm.callback(task.target)
		rm.mu.Lock()
		if err != nil {
			task.state.ConsecutiveFails++
			next := time.Duration(float64(task.state.CurrentInterval) * rm.config.Multiplier)
			if next > rm.config.MaxInterval {
				next = rm.config.MaxInterval
			}
			task.state.CurrentInterval = next
		} else {
			task.state.Connected = true
			task.state.ConsecutiveFails = 0
			task.state.CurrentInterval = rm.config.InitialInterval
		}
		rm.mu.Unlock()
	}
}
