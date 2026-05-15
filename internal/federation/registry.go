package federation

import (
	"log"
	"sync"
	"time"
)

// ClusterStatus represents the health state of a remote cluster.
type ClusterStatus string

const (
	ClusterAvailable   ClusterStatus = "available"
	ClusterUnavailable ClusterStatus = "unavailable"
)

// RemoteCluster represents a registered remote cluster.
type RemoteCluster struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Endpoint    string        `json:"endpoint"`
	Status      ClusterStatus `json:"status"`
	RegisteredAt time.Time    `json:"registered_at"`
	LastPing    time.Time     `json:"last_ping"`
	PingLatency time.Duration `json:"ping_latency"`
}

// Registry manages registered remote clusters.
type Registry struct {
	mu       sync.RWMutex
	clusters map[string]*RemoteCluster
}

// NewRegistry creates a new federation registry.
func NewRegistry() *Registry {
	return &Registry{clusters: make(map[string]*RemoteCluster)}
}

// Register adds or updates a remote cluster.
func (r *Registry) Register(id, name, endpoint string) *RemoteCluster {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.clusters[id]; ok {
		existing.Name = name
		existing.Endpoint = endpoint
		existing.Status = ClusterAvailable
		existing.LastPing = time.Now()
		log.Printf("federation: updated cluster %s (%s)", id, name)
		return existing
	}

	c := &RemoteCluster{
		ID:           id,
		Name:         name,
		Endpoint:     endpoint,
		Status:       ClusterAvailable,
		RegisteredAt: time.Now(),
		LastPing:     time.Now(),
	}
	r.clusters[id] = c
	log.Printf("federation: registered cluster %s (%s) at %s", id, name, endpoint)
	return c
}

// Get returns a remote cluster by ID.
func (r *Registry) Get(id string) (*RemoteCluster, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.clusters[id]
	if !ok {
		return nil, false
	}
	cp := *c
	return &cp, true
}

// GetAll returns all registered remote clusters.
func (r *Registry) GetAll() []*RemoteCluster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]*RemoteCluster, 0, len(r.clusters))
	for _, c := range r.clusters {
		cp := *c
		result = append(result, &cp)
	}
	return result
}

// Remove removes a remote cluster.
func (r *Registry) Remove(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.clusters[id]; !ok {
		return false
	}
	delete(r.clusters, id)
	log.Printf("federation: removed cluster %s", id)
	return true
}

// MarkUnavailable marks a cluster as unavailable.
func (r *Registry) MarkUnavailable(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clusters[id]; ok {
		c.Status = ClusterUnavailable
	}
}

// MarkAvailable marks a cluster as available and updates ping info.
func (r *Registry) MarkAvailable(id string, latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clusters[id]; ok {
		c.Status = ClusterAvailable
		c.LastPing = time.Now()
		c.PingLatency = latency
	}
}

// Count returns the total number of registered clusters.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clusters)
}

// AvailableCount returns the number of available clusters.
func (r *Registry) AvailableCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, c := range r.clusters {
		if c.Status == ClusterAvailable {
			count++
		}
	}
	return count
}
