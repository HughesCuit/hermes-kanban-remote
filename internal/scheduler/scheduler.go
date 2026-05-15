package scheduler

import (
	"log"
	"sync"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/capability"
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
)

// SchedulingDecision records one scheduling decision for audit/traceability.
type SchedulingDecision struct {
	TaskID      string    `json:"task_id"`
	TaskTitle   string    `json:"task_title"`
	Priority    int       `json:"priority"`
	NodeID      string    `json:"node_id"`
	Score       float64   `json:"score"`
	Reason      string    `json:"reason"`
	Timestamp   time.Time `json:"timestamp"`
}

// SchedulingStats holds aggregate scheduling metrics.
type SchedulingStats struct {
	TotalDecisions      int            `json:"total_decisions"`
	DecisionsByPriority map[int]int    `json:"decisions_by_priority"`
	AvgWaitTimeMs       float64        `json:"avg_wait_time_ms"`
	FailedSchedules     int            `json:"failed_schedules"`
	FailureReasons      map[string]int `json:"failure_reasons"`
	LastDecisions       []SchedulingDecision `json:"last_decisions,omitempty"` // most recent N
}

// Scheduler orchestrates task assignment to nodes with priority + load awareness.
type Scheduler struct {
	registry   *cluster.Registry
	taskStore  *TaskStore
	leaseMgr   *lease.Manager
	leaseTTL   time.Duration
	decisionMu sync.RWMutex
	decisions  []SchedulingDecision // bounded ring buffer
	maxDecisions int
}

// NewScheduler creates a new scheduler.
func NewScheduler(registry *cluster.Registry, taskStore *TaskStore, leaseMgr *lease.Manager, leaseTTL time.Duration) *Scheduler {
	return &Scheduler{
		registry:     registry,
		taskStore:    taskStore,
		leaseMgr:     leaseMgr,
		leaseTTL:     leaseTTL,
		maxDecisions: 200, // keep last 200 decisions
	}
}

// recordDecision appends a scheduling decision to the ring buffer.
func (s *Scheduler) recordDecision(d SchedulingDecision) {
	s.decisionMu.Lock()
	defer s.decisionMu.Unlock()
	s.decisions = append(s.decisions, d)
	if len(s.decisions) > s.maxDecisions {
		s.decisions = s.decisions[len(s.decisions)-s.maxDecisions:]
	}
}

// buildNodeInfos constructs scoring inputs from online nodes with load data.
func (s *Scheduler) buildNodeInfos() []capability.NodeInfo {
	nodes := s.registry.GetAll()
	var nodeInfos []capability.NodeInfo
	for _, n := range nodes {
		if n.Status == cluster.NodeOnline {
			activeTasks := s.taskStore.ActiveCountForNode(n.ID)
			nodeInfos = append(nodeInfos, capability.NodeInfo{
				ID:           n.ID,
				Capabilities: n.Capabilities,
				Load:         n.Load,
				HeartbeatAge: time.Since(n.LastHeartbeat).Seconds(),
				ActiveTasks:  activeTasks,
				AvgCompletion: 0, // TODO: track historical completion time
			})
		}
	}
	return nodeInfos
}

// TriggerPendingTasks scans pending tasks and promotes to ready if matching nodes exist.
// Returns list of task IDs that were promoted.
func (s *Scheduler) TriggerPendingTasks() []string {
	pending := s.taskStore.GetPending() // sorted by priority, then FIFO
	nodeInfos := s.buildNodeInfos()

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

// SchedulePending picks ready tasks (sorted by priority then FIFO) and assigns
// them to the best matching node using least-loaded-first + capability scoring.
// Returns list of (taskID, nodeID) pairs that were scheduled.
func (s *Scheduler) SchedulePending() [][2]string {
	ready := s.taskStore.GetReady() // sorted by priority (ascending), then FIFO
	nodeInfos := s.buildNodeInfos()

	var scheduled [][2]string
	for _, task := range ready {
		if len(nodeInfos) == 0 {
			// Record failure: no online nodes
			s.recordDecision(SchedulingDecision{
				TaskID:    task.ID,
				TaskTitle: task.Title,
				Priority:  task.Priority,
				NodeID:    "",
				Score:     0,
				Reason:    "no_online_nodes",
				Timestamp: time.Now(),
			})
			continue
		}

		ranked := capability.RankNodes(nodeInfos, task.Requires)
		if len(ranked) == 0 {
			// Record failure: no capable nodes
			s.recordDecision(SchedulingDecision{
				TaskID:    task.ID,
				TaskTitle: task.Title,
				Priority:  task.Priority,
				NodeID:    "",
				Score:     0,
				Reason:    "no_capable_nodes",
				Timestamp: time.Now(),
			})
			continue
		}

		best := ranked[0]
		score := capability.Score(best, task.Requires)
		_, err := s.leaseMgr.Create(task.ID, best.ID, s.leaseTTL)
		if err != nil {
			// Record failure: lease conflict
			s.recordDecision(SchedulingDecision{
				TaskID:    task.ID,
				TaskTitle: task.Title,
				Priority:  task.Priority,
				NodeID:    best.ID,
				Score:     score,
				Reason:    "lease_conflict: " + err.Error(),
				Timestamp: time.Now(),
			})
			continue // lease creation failed (e.g., already held)
		}

		s.taskStore.SetAssigned(task.ID, best.ID)

		// Record success
		s.recordDecision(SchedulingDecision{
			TaskID:    task.ID,
			TaskTitle: task.Title,
			Priority:  task.Priority,
			NodeID:    best.ID,
			Score:     score,
			Reason:    "priority_load_capable",
			Timestamp: time.Now(),
		})

		log.Printf("scheduled task %s (p%d, score=%.3f) → %s", task.ID, task.Priority, score, best.ID)
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

// GetStats returns aggregate scheduling statistics.
func (s *Scheduler) GetStats() SchedulingStats {
	s.decisionMu.RLock()
	defer s.decisionMu.RUnlock()

	stats := SchedulingStats{
		TotalDecisions:      len(s.decisions),
		DecisionsByPriority: make(map[int]int),
		FailureReasons:      make(map[string]int),
	}

	var totalWaitTime float64
	var waitCount int

	for _, d := range s.decisions {
		stats.DecisionsByPriority[d.Priority]++
		if d.NodeID == "" || d.Reason != "priority_load_capable" {
			stats.FailedSchedules++
			stats.FailureReasons[d.Reason]++
		}
	}

	// Calculate avg wait time from task creation to scheduling
	allTasks := s.taskStore.GetAll()
	for _, t := range allTasks {
		if t.Status == TaskAssigned && !t.CreatedAt.IsZero() && !t.UpdatedAt.IsZero() {
			waitMs := float64(t.UpdatedAt.Sub(t.CreatedAt).Milliseconds())
			totalWaitTime += waitMs
			waitCount++
		}
	}
	if waitCount > 0 {
		stats.AvgWaitTimeMs = totalWaitTime / float64(waitCount)
	}

	// Return last 50 decisions
	n := len(s.decisions)
	start := n - 50
	if start < 0 {
		start = 0
	}
	stats.LastDecisions = make([]SchedulingDecision, n-start)
	copy(stats.LastDecisions, s.decisions[start:])

	return stats
}

// GetDecisions returns the full decision log (capped at maxDecisions).
func (s *Scheduler) GetDecisions() []SchedulingDecision {
	s.decisionMu.RLock()
	defer s.decisionMu.RUnlock()
	result := make([]SchedulingDecision, len(s.decisions))
	copy(result, s.decisions)
	return result
}

// GetTaskStore returns the underlying task store.
func (s *Scheduler) GetTaskStore() *TaskStore {
	return s.taskStore
}
