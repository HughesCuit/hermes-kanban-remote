package scheduler

import (
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/capability"
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
)

// Scheduler orchestrates task assignment to nodes.
type Scheduler struct {
	registry   *cluster.Registry
	taskStore  *TaskStore
	leaseMgr   *lease.Manager
	leaseTTL   time.Duration
}

// NewScheduler creates a new scheduler.
func NewScheduler(registry *cluster.Registry, taskStore *TaskStore, leaseMgr *lease.Manager, leaseTTL time.Duration) *Scheduler {
	return &Scheduler{
		registry:  registry,
		taskStore: taskStore,
		leaseMgr:  leaseMgr,
		leaseTTL:  leaseTTL,
	}
}

// TriggerPendingTasks scans pending tasks and promotes to ready if matching nodes exist.
// Returns list of task IDs that were promoted.
func (s *Scheduler) TriggerPendingTasks() []string {
	pending := s.taskStore.GetPending()
	nodes := s.registry.GetAll()

	// Build scoring inputs for online nodes
	var nodeInfos []capability.NodeInfo
	for _, n := range nodes {
		if n.Status == cluster.NodeOnline {
			nodeInfos = append(nodeInfos, capability.NodeInfo{
				ID:           n.ID,
				Capabilities: n.Capabilities,
				Load:         n.Load,
				HeartbeatAge: time.Since(n.LastHeartbeat).Seconds(),
			})
		}
	}

	var promoted []string
	for _, task := range pending {
		ranked := capability.RankNodes(nodeInfos, task.Requires)
		if len(ranked) > 0 {
			if s.taskStore.PromoteIfPending(task.ID, TaskReady) {
				promoted = append(promoted, task.ID)
			}
		}
	}
	return promoted
}

// SchedulePending picks ready tasks and assigns them to the best matching node.
// Returns list of (taskID, nodeID) pairs that were scheduled.
func (s *Scheduler) SchedulePending() [][2]string {
	ready := s.taskStore.GetReady()
	nodes := s.registry.GetAll()

	// Build scoring inputs
	var nodeInfos []capability.NodeInfo
	for _, n := range nodes {
		if n.Status == cluster.NodeOnline {
			nodeInfos = append(nodeInfos, capability.NodeInfo{
				ID:           n.ID,
				Capabilities: n.Capabilities,
				Load:         n.Load,
				HeartbeatAge: time.Since(n.LastHeartbeat).Seconds(),
			})
		}
	}

	var scheduled [][2]string
	for _, task := range ready {
		ranked := capability.RankNodes(nodeInfos, task.Requires)
		if len(ranked) == 0 {
			continue // no available node
		}

		best := ranked[0]
		_, err := s.leaseMgr.Create(task.ID, best.ID, s.leaseTTL)
		if err != nil {
			continue // lease creation failed (e.g., already held)
		}

		s.taskStore.SetAssigned(task.ID, best.ID)
		scheduled = append(scheduled, [2]string{task.ID, best.ID})
	}

	return scheduled
}

// RescheduleTask releases a task and reassigns it.
func (s *Scheduler) RescheduleTask(taskID string) (string, error) {
	// Get current assignment
	t, ok := s.taskStore.Get(taskID)
	if !ok {
		return "", nil
	}

	// Revoke existing lease if any
	if t.AssignedTo != "" {
		if activeLease, ok := s.leaseMgr.GetActiveForTask(taskID); ok {
			s.leaseMgr.Revoke(activeLease.ID)
		}
		s.taskStore.Unassign(taskID)
	}

	// Try to reschedule
	scheduled := s.SchedulePending()
	for _, pair := range scheduled {
		if pair[0] == taskID {
			return pair[1], nil
		}
	}
	return "", nil // no node available
}

// GetTaskStore returns the underlying task store.
func (s *Scheduler) GetTaskStore() *TaskStore {
	return s.taskStore
}
