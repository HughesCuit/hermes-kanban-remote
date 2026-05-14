package cluster

import "github.com/heventure/hermes-agent-cluster/internal/heartbeat"

// RegistryAdapter bridges cluster.Registry to the heartbeat.HeartbeatRegistry interface.
// Used by heartbeat.Watchdog to observe node statuses from the cluster registry.
type RegistryAdapter struct {
	Reg *Registry
}

// GetAll returns all registered nodes as HeartbeatNodes.
func (ra *RegistryAdapter) GetAll() []heartbeat.HeartbeatNode {
	nodes := ra.Reg.GetAll()
	result := make([]heartbeat.HeartbeatNode, len(nodes))
	for i, n := range nodes {
		result[i] = heartbeat.HeartbeatNode{
			ID:            n.ID,
			LastHeartbeat: n.LastHeartbeat,
			Status:        string(n.Status),
		}
	}
	return result
}

// UpdateStatus updates a node's status in the cluster registry.
func (ra *RegistryAdapter) UpdateStatus(id string, status string) {
	ra.Reg.UpdateStatus(id, NodeStatus(status))
}
