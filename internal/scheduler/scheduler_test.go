package scheduler

import (
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/capability"
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
)

// --- Priority field tests ---

func TestTask_DefaultPriority(t *testing.T) {
	store := NewTaskStore()
	task, err := store.Create("t1", "Default priority task", nil)
	if err != nil {
		t.Fatal(err)
	}
	if task.Priority != DefaultPriority {
		t.Errorf("expected default priority %d, got %d", DefaultPriority, task.Priority)
	}
}

func TestTask_ExplicitPriority(t *testing.T) {
	store := NewTaskStore()
	task, err := store.CreateWithPriority("t1", "High priority", nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if task.Priority != 1 {
		t.Errorf("expected priority 1, got %d", task.Priority)
	}
}

func TestTask_PriorityNormalization(t *testing.T) {
	store := NewTaskStore()

	// Priority 0 -> default (3)
	task, _ := store.CreateWithPriority("t1", "Zero priority", nil, 0)
	if task.Priority != DefaultPriority {
		t.Errorf("priority 0 should normalize to %d, got %d", DefaultPriority, task.Priority)
	}

	// Priority 6 -> clamped to 5
	task, _ = store.CreateWithPriority("t2", "Over max", nil, 6)
	if task.Priority != 5 {
		t.Errorf("priority 6 should clamp to 5, got %d", task.Priority)
	}

	// Priority -1 -> default (3)
	task, _ = store.CreateWithPriority("t3", "Negative", nil, -1)
	if task.Priority != DefaultPriority {
		t.Errorf("priority -1 should normalize to %d, got %d", DefaultPriority, task.Priority)
	}

	// Priority 3 (valid) stays 3
	task, _ = store.CreateWithPriority("t4", "Mid", nil, 3)
	if task.Priority != 3 {
		t.Errorf("priority 3 should stay 3, got %d", task.Priority)
	}
}

func TestGetReady_SortByPriorityThenFIFO(t *testing.T) {
	store := NewTaskStore()

	// Create tasks with different priorities and timestamps
	store.CreateWithPriority("t_low", "Low priority", nil, 5)
	time.Sleep(2 * time.Millisecond)
	store.CreateWithPriority("t_high", "High priority", nil, 1)
	time.Sleep(2 * time.Millisecond)
	store.CreateWithPriority("t_mid", "Mid priority", nil, 3)
	time.Sleep(2 * time.Millisecond)
	// Another high priority task (should come after t_high due to FIFO)
	store.CreateWithPriority("t_high2", "High priority 2", nil, 1)

	ready := store.GetReady()
	if len(ready) != 4 {
		t.Fatalf("expected 4 ready tasks, got %d", len(ready))
	}

	// Verify ordering: priority 1 first, then 3, then 5
	expectedOrder := []string{"t_high", "t_high2", "t_mid", "t_low"}
	for i, id := range expectedOrder {
		if ready[i].ID != id {
			t.Errorf("position %d: expected %s, got %s", i, id, ready[i].ID)
		}
	}
}

// --- Scorer tests ---

func TestScore_WithActiveTasks(t *testing.T) {
	n1 := capability.NodeInfo{
		ID:           "n1",
		Capabilities: []string{"python"},
		Load:         0.0,
		HeartbeatAge: 0,
		ActiveTasks:  5,
		MaxCapacity:  10,
	}

	n2 := capability.NodeInfo{
		ID:           "n2",
		Capabilities: []string{"python"},
		Load:         0.0,
		HeartbeatAge: 0,
		ActiveTasks:  1,
		MaxCapacity:  10,
	}

	s1 := capability.Score(n1, []string{"python"})
	s2 := capability.Score(n2, []string{"python"})

	if s2 <= s1 {
		t.Errorf("node with fewer active tasks should score higher: n1=%.3f, n2=%.3f", s1, s2)
	}
}

func TestScore_AtCapacity(t *testing.T) {
	n := capability.NodeInfo{
		ID:           "n_full",
		Capabilities: []string{"python"},
		Load:         0.0,
		HeartbeatAge: 0,
		ActiveTasks:  10,
		MaxCapacity:  10,
	}

	s := capability.Score(n, []string{"python"})
	// At capacity: active_score=0, so max possible = 0.4+0.25+0.15 = 0.8
	if s > 0.81 {
		t.Errorf("node at capacity should have reduced score (active_score=0), got %.3f", s)
	}

	// Compare with a node not at capacity
	n2 := capability.NodeInfo{
		ID:           "n_ok",
		Capabilities: []string{"python"},
		Load:         0.0,
		HeartbeatAge: 0,
		ActiveTasks:  5,
		MaxCapacity:  10,
	}
	s2 := capability.Score(n2, []string{"python"})
	if s >= s2 {
		t.Errorf("node at capacity (%.3f) should score lower than node at 50%% (%.3f)", s, s2)
	}
}

func TestScore_NoCapacityLimit(t *testing.T) {
	n := capability.NodeInfo{
		ID:           "n_unlimited",
		Capabilities: []string{"python"},
		Load:         0.0,
		HeartbeatAge: 0,
		ActiveTasks:  3,
		MaxCapacity:  0, // unlimited
	}

	s := capability.Score(n, []string{"python"})
	if s <= 0 {
		t.Errorf("unlimited node with some tasks should still score positive, got %.3f", s)
	}
}

// --- Scheduler integration tests ---

func setupScheduler() (*Scheduler, *cluster.Registry, *TaskStore, *lease.Manager) {
	reg := cluster.NewRegistry()
	store := NewTaskStore()
	lmgr := lease.NewManager()
	sched := NewScheduler(reg, store, lmgr, 5*time.Minute)
	return sched, reg, store, lmgr
}

func registerNode(reg *cluster.Registry, id string, caps []string) {
	reg.Register(&cluster.Node{
		ID:           id,
		Name:         id,
		Capabilities: caps,
	})
}

func TestSchedulePending_HighPriorityFirst(t *testing.T) {
	sched, reg, store, _ := setupScheduler()

	// Register a node
	registerNode(reg, "node1", []string{"python", "docker"})

	// Create tasks: low priority first, then high priority
	store.CreateWithPriority("t_low", "Low task", []string{"python"}, 5)
	time.Sleep(2 * time.Millisecond)
	store.CreateWithPriority("t_high", "High task", []string{"python"}, 1)

	// Schedule
	scheduled := sched.SchedulePending()
	if len(scheduled) != 2 {
		t.Fatalf("expected 2 scheduled tasks, got %d", len(scheduled))
	}

	// High priority should be scheduled first
	if scheduled[0][0] != "t_high" {
		t.Errorf("high priority task should be scheduled first, got %s", scheduled[0][0])
	}
	if scheduled[1][0] != "t_low" {
		t.Errorf("low priority task should be scheduled second, got %s", scheduled[1][0])
	}
}

func TestSchedulePending_LeastLoadedNode(t *testing.T) {
	sched, reg, store, _ := setupScheduler()

	// Register two nodes with different loads
	registerNode(reg, "node_heavy", []string{"python"})
	reg.SetLoad("node_heavy", 0.8)

	registerNode(reg, "node_light", []string{"python"})
	reg.SetLoad("node_light", 0.1)

	// Create a task
	store.CreateWithPriority("t1", "Task", []string{"python"}, 3)

	// Schedule
	scheduled := sched.SchedulePending()
	if len(scheduled) != 1 {
		t.Fatalf("expected 1 scheduled task, got %d", len(scheduled))
	}

	// Should go to the lighter node
	if scheduled[0][1] != "node_light" {
		t.Errorf("task should go to least-loaded node, got %s", scheduled[0][1])
	}
}

func TestSchedulePending_DecisionLog(t *testing.T) {
	sched, reg, store, _ := setupScheduler()

	registerNode(reg, "node1", []string{"python"})
	store.CreateWithPriority("t1", "Task", []string{"python"}, 1)

	sched.SchedulePending()

	decisions := sched.GetDecisions()
	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision logged, got %d", len(decisions))
	}

	d := decisions[0]
	if d.TaskID != "t1" {
		t.Errorf("expected task_id=t1, got %s", d.TaskID)
	}
	if d.Priority != 1 {
		t.Errorf("expected priority=1, got %d", d.Priority)
	}
	if d.NodeID != "node1" {
		t.Errorf("expected node_id=node1, got %s", d.NodeID)
	}
	if d.Reason != "priority_load_capable" {
		t.Errorf("expected reason=priority_load_capable, got %s", d.Reason)
	}
}

func TestSchedulePending_NoCapableNode_Logged(t *testing.T) {
	sched, reg, store, _ := setupScheduler()

	// Node has no required capability
	registerNode(reg, "node1", []string{"java"})
	store.CreateWithPriority("t1", "Python task", []string{"python"}, 1)

	scheduled := sched.SchedulePending()
	if len(scheduled) != 0 {
		t.Errorf("expected 0 scheduled, got %d", len(scheduled))
	}

	decisions := sched.GetDecisions()
	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(decisions))
	}
	if decisions[0].Reason != "no_capable_nodes" {
		t.Errorf("expected reason=no_capable_nodes, got %s", decisions[0].Reason)
	}
}

func TestScheduleStats(t *testing.T) {
	sched, reg, store, _ := setupScheduler()

	registerNode(reg, "node1", []string{"python"})

	// Schedule 3 tasks
	store.CreateWithPriority("t1", "P1", []string{"python"}, 1)
	store.CreateWithPriority("t2", "P3", []string{"python"}, 3)
	store.CreateWithPriority("t3", "P1", []string{"python"}, 1)

	sched.SchedulePending()

	stats := sched.GetStats()
	if stats.TotalDecisions != 3 {
		t.Errorf("expected 3 total decisions, got %d", stats.TotalDecisions)
	}
	if stats.DecisionsByPriority[1] != 2 {
		t.Errorf("expected 2 decisions for priority 1, got %d", stats.DecisionsByPriority[1])
	}
	if stats.DecisionsByPriority[3] != 1 {
		t.Errorf("expected 1 decision for priority 3, got %d", stats.DecisionsByPriority[3])
	}
	if stats.FailedSchedules != 0 {
		t.Errorf("expected 0 failed, got %d", stats.FailedSchedules)
	}
}

func TestScheduleStats_Failures(t *testing.T) {
	sched, _, store, _ := setupScheduler()

	// No nodes registered
	store.CreateWithPriority("t1", "Task", []string{"python"}, 1)

	sched.SchedulePending()

	stats := sched.GetStats()
	if stats.FailedSchedules != 1 {
		t.Errorf("expected 1 failure, got %d", stats.FailedSchedules)
	}
	if stats.FailureReasons["no_online_nodes"] != 1 {
		t.Errorf("expected no_online_nodes failure, got %v", stats.FailureReasons)
	}
}

func TestBackwardCompat_NoPriorityField(t *testing.T) {
	store := NewTaskStore()

	// Old API: Create() with no priority — should default to 3
	task, err := store.Create("t_old", "Old-style task", []string{"python"})
	if err != nil {
		t.Fatal(err)
	}
	if task.Priority != DefaultPriority {
		t.Errorf("backward compat: expected default priority %d, got %d", DefaultPriority, task.Priority)
	}
}

func TestSchedulePending_EmptyStore(t *testing.T) {
	sched, _, _, _ := setupScheduler()

	registerNode(sched.registry, "node1", []string{"python"})
	// No tasks

	scheduled := sched.SchedulePending()
	if len(scheduled) != 0 {
		t.Errorf("expected 0 scheduled tasks from empty store, got %d", len(scheduled))
	}
}

func TestSchedulePending_NoOnlineNodes(t *testing.T) {
	sched, reg, store, _ := setupScheduler()

	// Register a node but make it offline
	node := &cluster.Node{
		ID:           "node1",
		Name:         "node1",
		Capabilities: []string{"python"},
	}
	reg.Register(node)
	reg.UpdateStatus("node1", cluster.NodeOffline)

	store.CreateWithPriority("t1", "Task", []string{"python"}, 1)

	scheduled := sched.SchedulePending()
	if len(scheduled) != 0 {
		t.Errorf("expected 0 scheduled (offline node), got %d", len(scheduled))
	}
}
