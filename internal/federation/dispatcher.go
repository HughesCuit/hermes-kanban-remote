package federation

import (
	"log"
	"sync"
	"time"
)

// Dispatcher manages forwarding tasks to remote clusters and periodic health checks.
type Dispatcher struct {
	registry *Registry
	client   *Client
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewDispatcher creates a new federation dispatcher.
func NewDispatcher(registry *Registry, client *Client) *Dispatcher {
	return &Dispatcher{
		registry: registry,
		client:   client,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the periodic health check loop for remote clusters.
// checkInterval controls how often remote clusters are pinged.
func (d *Dispatcher) Start(checkInterval time.Duration) {
	d.wg.Add(1)
	go d.healthCheckLoop(checkInterval)
}

// Stop terminates the background health check loop and waits for
// all in-flight ping goroutines to complete.
func (d *Dispatcher) Stop() {
	close(d.stopCh)
	d.wg.Wait()
}

// healthCheckLoop periodically pings all registered remote clusters.
func (d *Dispatcher) healthCheckLoop(interval time.Duration) {
	defer d.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.pingAll()
		case <-d.stopCh:
			return
		}
	}
}

// pingAll pings every registered remote cluster and updates their status.
func (d *Dispatcher) pingAll() {
	clusters := d.registry.GetAll()
	for _, c := range clusters {
		d.wg.Add(1)
		go func(cluster *RemoteCluster) {
			defer d.wg.Done()
			_, latency, err := d.client.Ping(cluster.Endpoint)
			if err != nil {
				log.Printf("federation: ping failed for %s (%s): %v", cluster.ID, cluster.Endpoint, err)
				d.registry.MarkUnavailable(cluster.ID)
			} else {
				d.registry.MarkAvailable(cluster.ID, latency)
			}
		}(c)
	}
}

// ForwardTask forwards a task to a specific remote cluster by ID.
// Returns the remote task ID on success.
func (d *Dispatcher) ForwardTask(clusterID, title string, requires []string) (string, error) {
	cluster, ok := d.registry.Get(clusterID)
	if !ok {
		return "", ErrClusterNotFound
	}
	if cluster.Status == ClusterUnavailable {
		return "", ErrClusterUnavailable
	}

	result, err := d.client.ForwardTask(cluster.Endpoint, title, requires)
	if err != nil {
		d.registry.MarkUnavailable(clusterID)
		return "", err
	}
	log.Printf("federation: forwarded task to cluster %s: local=? remote=%s", clusterID, result.ID)
	return result.ID, nil
}

// QueryClusterStatus queries the status of a remote cluster.
func (d *Dispatcher) QueryClusterStatus(clusterID string) (*StatusResponse, error) {
	cluster, ok := d.registry.Get(clusterID)
	if !ok {
		return nil, ErrClusterNotFound
	}
	return d.client.QueryStatus(cluster.Endpoint)
}

// Sentinel errors for federation operations.
type FederationError string

func (e FederationError) Error() string { return string(e) }

const (
	ErrClusterNotFound    FederationError = "cluster not found"
	ErrClusterUnavailable FederationError = "cluster unavailable"
)
