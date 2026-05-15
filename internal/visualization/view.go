package visualization

import (
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/recovery"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
	"github.com/heventure/hermes-agent-cluster/internal/workflow"
)

// ClusterView ties together all cluster data sources for visualization.
type ClusterView struct {
	Registry  *cluster.Registry
	TaskStore *scheduler.TaskStore
	LeaseMgr  *lease.Manager
	RecLog    *recovery.Log
	Resolver  *workflow.Resolver
}

// NewClusterView creates a new ClusterView.
func NewClusterView(
	registry *cluster.Registry,
	taskStore *scheduler.TaskStore,
	leaseMgr *lease.Manager,
	recLog *recovery.Log,
	resolver *workflow.Resolver,
) *ClusterView {
	return &ClusterView{
		Registry:  registry,
		TaskStore: taskStore,
		LeaseMgr:  leaseMgr,
		RecLog:    recLog,
		Resolver:  resolver,
	}
}

// GetTopology returns the full cluster topology.
func (cv *ClusterView) GetTopology() ClusterTopology {
	nodes := cv.Registry.GetAll()
	tasks := cv.TaskStore.GetAll()
	leases := cv.LeaseMgr.GetActiveLeases()

	taskCount := make(map[string]int)
	for _, l := range leases {
		taskCount[l.NodeID]++
	}

	topoNodes := make([]TopologyNode, len(nodes))
	for i, n := range nodes {
		topoNodes[i] = TopologyNode{
			ID:            n.ID,
			Name:          n.Name,
			Status:        n.Status,
			Capabilities:  n.Capabilities,
			Load:          n.Load,
			AssignedTasks: taskCount[n.ID],
		}
	}

	topoTasks := make([]TopologyTask, len(tasks))
	for i, t := range tasks {
		topoTasks[i] = TopologyTask{
			ID:              t.ID,
			Title:           t.Title,
			Status:          t.Status,
			AssignedTo:      t.AssignedTo,
			DependencyCount: len(t.DependsOn),
		}
	}

	var edges []TopologyEdge
	for _, l := range leases {
		edges = append(edges, TopologyEdge{
			From: l.NodeID,
			To:   l.TaskID,
			Type: "assignment",
		})
	}
	for _, t := range tasks {
		for _, dep := range t.DependsOn {
			edges = append(edges, TopologyEdge{
				From: t.ID,
				To:   dep,
				Type: "dependency",
			})
		}
	}

	return ClusterTopology{
		Nodes: topoNodes,
		Tasks: topoTasks,
		Edges: edges,
	}
}

// GetMetrics returns aggregate cluster metrics.
func (cv *ClusterView) GetMetrics() ClusterMetrics {
	nodes := cv.Registry.GetAll()
	tasks := cv.TaskStore.GetAll()
	leases := cv.LeaseMgr.GetActiveLeases()

	nodeMetrics := make([]NodeMetric, len(nodes))
	taskCount := make(map[string]int)
	for _, l := range leases {
		taskCount[l.NodeID]++
	}
	for i, n := range nodes {
		nodeMetrics[i] = NodeMetric{
			ID:            n.ID,
			Name:          n.Name,
			Status:        string(n.Status),
			TasksAssigned: taskCount[n.ID],
			Load:          n.Load,
		}
	}

	byStatus := make(map[string]int)
	for _, t := range tasks {
		byStatus[string(t.Status)]++
	}
	total := len(tasks)
	var completionRate float64
	if total > 0 {
		completionRate = float64(byStatus[string(scheduler.TaskCompleted)]) / float64(total)
	}

	recStats := cv.RecLog.Stats()

	return ClusterMetrics{
		Nodes: nodeMetrics,
		Tasks: TaskMetric{
			Total:          total,
			ByStatus:       byStatus,
			CompletionRate: completionRate,
		},
		Leases: LeaseMetric{
			ActiveCount:  len(leases),
			ExpiredCount: recStats["failed"],
		},
	}
}

// GetTimeline returns recent cluster events.
func (cv *ClusterView) GetTimeline(limit int) ClusterTimeline {
	events := cv.RecLog.GetEvents()

	timeline := make([]TimelineEvent, 0, len(events))
	for _, e := range events {
		timeline = append(timeline, TimelineEvent{
			Type:        "recovery",
			Timestamp:   e.Timestamp,
			NodeID:      e.NodeID,
			TaskID:      e.TaskID,
			Description: e.Action + ": " + e.Message,
		})
	}

	if limit > 0 && len(timeline) > limit {
		timeline = timeline[len(timeline)-limit:]
	}

	return ClusterTimeline{Events: timeline}
}
