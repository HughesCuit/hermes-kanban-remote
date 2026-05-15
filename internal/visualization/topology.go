package visualization

import (
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

// ClusterTopology is the full topology view of the cluster.
type ClusterTopology struct {
	Nodes []TopologyNode `json:"nodes"`
	Tasks []TopologyTask `json:"tasks"`
	Edges []TopologyEdge `json:"edges"`
}

// TopologyNode represents a node in the cluster topology.
type TopologyNode struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Status        cluster.NodeStatus `json:"status"`
	Capabilities  []string        `json:"capabilities"`
	Load          float64         `json:"load"`
	AssignedTasks int             `json:"assigned_tasks"`
}

// TopologyTask represents a task in the cluster topology.
type TopologyTask struct {
	ID              string              `json:"id"`
	Title           string              `json:"title"`
	Status          scheduler.TaskStatus `json:"status"`
	AssignedTo      string              `json:"assigned_to"`
	DependencyCount int                 `json:"dependency_count"`
}

// TopologyEdge represents a connection between elements.
type TopologyEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Type string `json:"type"` // "assignment" or "dependency"
}
