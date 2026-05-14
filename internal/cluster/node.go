package cluster

import (
	"sync"
	"time"
)

// NodeStatus represents the health state of a node.
type NodeStatus string

const (
	NodeOnline   NodeStatus = "online"
	NodeDegraded NodeStatus = "degraded"
	NodeOffline  NodeStatus = "offline"
)

// Node represents a cluster member.
type Node struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	Capabilities  []string   `json:"capabilities"`
	Status        NodeStatus `json:"status"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	Load          float64    `json:"load"` // 0.0 - 1.0
}

// Registry manages cluster nodes.
type Registry struct {
	mu            sync.RWMutex
	nodes         map[string]*Node
	onNodeOnline  func(nodeID string)
	onCapabilityChange func(nodeID string, oldCaps, newCaps []string)
}

// NewRegistry creates a new node registry.
func NewRegistry() *Registry {
	return &Registry{nodes: make(map[string]*Node)}
}

// SetOnNodeOnline registers a callback to be called when a node comes online.
func (r *Registry) SetOnNodeOnline(fn func(nodeID string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onNodeOnline = fn
}

// SetOnCapabilityChange registers a callback for when a node's capabilities change.
func (r *Registry) SetOnCapabilityChange(fn func(nodeID string, oldCaps, newCaps []string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onCapabilityChange = fn
}

// Register adds or updates a node.
func (r *Registry) Register(n *Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.nodes[n.ID]; ok {
		existing.Name = n.Name
		existing.Capabilities = n.Capabilities
		existing.Status = NodeOnline
		existing.LastHeartbeat = time.Now()
		if r.onNodeOnline != nil {
			go r.onNodeOnline(n.ID)
		}
		return
	}
	n.Status = NodeOnline
	n.LastHeartbeat = time.Now()
	r.nodes[n.ID] = n
	if r.onNodeOnline != nil {
		go r.onNodeOnline(n.ID)
	}
}

// Get returns a node by ID.
func (r *Registry) Get(id string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

// GetAll returns all registered nodes.
func (r *Registry) GetAll() []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		cp := *n
		result = append(result, &cp)
	}
	return result
}

// UpdateCapabilities updates a node's capabilities and fires the callback if changed.
func (r *Registry) UpdateCapabilities(id string, capabilities []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return
	}
	oldCaps := n.Capabilities
	// Compare sets to avoid spurious callbacks
	if capsEqual(oldCaps, capabilities) {
		return
	}
	n.Capabilities = make([]string, len(capabilities))
	copy(n.Capabilities, capabilities)
	if r.onCapabilityChange != nil {
		go r.onCapabilityChange(id, oldCaps, capabilities)
	}
}

// capsEqual returns true if two string slices contain the same elements (as sets).
func capsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if !set[s] {
			return false
		}
	}
	return true
}

// UpdateStatus sets the status of a node.
func (r *Registry) UpdateStatus(id string, status NodeStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[id]; ok {
		n.Status = status
	}
}

// UpdateHeartbeat updates the last heartbeat time.
func (r *Registry) UpdateHeartbeat(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[id]; ok {
		wasOnline := n.Status == NodeOnline
		n.LastHeartbeat = time.Now()
		n.Status = NodeOnline
		if !wasOnline && r.onNodeOnline != nil {
			go r.onNodeOnline(id)
		}
	}
}

// SetLoad updates the load metric for a node.
func (r *Registry) SetLoad(id string, load float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[id]; ok {
		n.Load = load
	}
}

// Remove removes a node.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, id)
}

// Count returns the number of registered nodes.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// OnlineCount returns the number of online nodes.
func (r *Registry) OnlineCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, n := range r.nodes {
		if n.Status == NodeOnline {
			count++
		}
	}
	return count
}
