package hooks

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Hook represents a registered webhook subscription.
type Hook struct {
	ID        string     `json:"id"`
	URL       string     `json:"url"`
	Events    []EventType `json:"events"`
	Secret    string     `json:"secret,omitempty"` // HMAC-SHA256 secret (omitted in list responses)
	Active    bool       `json:"active"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// Manager manages webhook registrations and dispatches events to subscribers.
type Manager struct {
	mu          sync.RWMutex
	hooks       map[string]*Hook
	deliveries  []*Delivery // delivery history (capped)
	maxHistory  int
	dispatcher  *Dispatcher
}

// NewManager creates a new webhook manager with the given dispatcher.
func NewManager(dispatcher *Dispatcher, maxHistory int) *Manager {
	if maxHistory <= 0 {
		maxHistory = 1000
	}
	return &Manager{
		hooks:      make(map[string]*Hook),
		deliveries: make([]*Delivery, 0, maxHistory),
		maxHistory: maxHistory,
		dispatcher: dispatcher,
	}
}

// generateID creates a random hex ID for hooks and deliveries.
func generateID(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(b)
}

// Register adds a new webhook subscription. Returns the created Hook.
func (m *Manager) Register(url string, events []EventType, secret string) (*Hook, error) {
	if url == "" {
		return nil, fmt.Errorf("url is required")
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("at least one event type is required")
	}
	for _, e := range events {
		if !IsValidEvent(e) {
			return nil, fmt.Errorf("invalid event type: %s", e)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	h := &Hook{
		ID:        generateID("hook"),
		URL:       url,
		Events:    events,
		Secret:    secret,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	m.hooks[h.ID] = h
	return h, nil
}

// Deregister removes a webhook by ID.
func (m *Manager) Deregister(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.hooks[id]; !ok {
		return fmt.Errorf("hook %s not found", id)
	}
	delete(m.hooks, id)
	return nil
}

// Get returns a hook by ID.
func (m *Manager) Get(id string) (*Hook, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.hooks[id]
	if !ok {
		return nil, false
	}
	cp := *h
	return &cp, true
}

// List returns all registered hooks (without secrets).
func (m *Manager) List() []*Hook {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Hook, 0, len(m.hooks))
	for _, h := range m.hooks {
		cp := *h
		cp.Secret = "" // omit secret in list responses
		result = append(result, &cp)
	}
	return result
}

// GetHooksForEvent returns all active hooks that subscribe to the given event type.
func (m *Manager) GetHooksForEvent(eventType EventType) []*Hook {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*Hook
	for _, h := range m.hooks {
		if !h.Active {
			continue
		}
		for _, e := range h.Events {
			if e == eventType {
				cp := *h
				result = append(result, &cp)
				break
			}
		}
	}
	return result
}

// Emit dispatches an event to all matching hooks asynchronously.
// Returns the number of hooks triggered.
func (m *Manager) Emit(eventType EventType, data interface{}) int {
	hooks := m.GetHooksForEvent(eventType)
	if len(hooks) == 0 {
		return 0
	}

	payload := Payload{
		EventType: eventType,
		Timestamp: time.Now().UTC(),
		Data:      data,
	}

	for _, h := range hooks {
		hookCopy := *h // capture for closure
		m.dispatcher.Deliver(hookCopy, payload, m.recordDelivery)
	}
	return len(hooks)
}

// recordDelivery appends a delivery record to history (thread-safe).
func (m *Manager) recordDelivery(d *Delivery) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Cap history to maxHistory
	if len(m.deliveries) >= m.maxHistory {
		// Remove oldest (FIFO)
		m.deliveries = m.deliveries[1:]
	}
	m.deliveries = append(m.deliveries, d)
}

// GetDeliveries returns delivery history for a specific hook.
func (m *Manager) GetDeliveries(hookID string) []*Delivery {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*Delivery
	for _, d := range m.deliveries {
		if d.HookID == hookID {
			cp := *d
			result = append(result, &cp)
		}
	}
	return result
}

// GetDeliveriesAll returns all delivery history (capped by maxHistory).
func (m *Manager) GetDeliveriesAll() []*Delivery {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Delivery, len(m.deliveries))
	for i, d := range m.deliveries {
		cp := *d
		result[i] = &cp
	}
	return result
}
