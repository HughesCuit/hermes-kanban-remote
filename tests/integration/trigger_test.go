package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/api"
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/recovery"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
	"github.com/heventure/hermes-agent-cluster/internal/sync"
	"github.com/heventure/hermes-agent-cluster/internal/workflow"
)

// TriggerTestCluster is a simplified cluster for trigger testing.
type TriggerTestCluster struct {
	Registry    *cluster.Registry
	Scheduler   *scheduler.Scheduler
	LeaseMgr    *lease.Manager
	TaskStore   *scheduler.TaskStore
	RecLog      *recovery.Log
	StateStore  *sync.StateStore
	Receiver    *sync.FollowerReceiver
	LeaderSync  *sync.LeaderSync
	Server      *api.Server
}

func NewTriggerTestCluster(leaseTTL time.Duration) *TriggerTestCluster {
	registry := cluster.NewRegistry()
	taskStore := scheduler.NewTaskStore()
	leaseMgr := lease.NewManager()
	recLog := recovery.NewLog()
	ss := sync.NewStateStore()
	receiver := sync.NewFollowerReceiver(ss)
	pusher := sync.NewHTTPPusher()
	leaderSync := sync.NewLeaderSync(ss, pusher)

	sched := scheduler.NewScheduler(registry, taskStore, leaseMgr, leaseTTL)
	revoker := recovery.NewRevoker(leaseMgr, recLog)
	resched := recovery.NewRescheduler(sched, recLog)
	detector := recovery.NewDetector(revoker, resched, leaseMgr, recLog)
	resolver := workflow.NewResolver(taskStore)

	server := api.NewServer(registry, sched, leaseMgr, detector, recLog, ss, receiver, leaderSync, resolver)

	// Wire up the trigger callback
	registry.SetOnNodeOnline(func(nodeID string) {
		promoted := sched.TriggerPendingTasks()
		if len(promoted) > 0 {
			sched.SchedulePending()
		}
	})

	return &TriggerTestCluster{
		Registry:   registry,
		Scheduler:  sched,
		LeaseMgr:   leaseMgr,
		TaskStore:  taskStore,
		RecLog:     recLog,
		StateStore: ss,
		Receiver:   receiver,
		LeaderSync: leaderSync,
		Server:     server,
	}
}

// TestTriggerPendingTasks_PromotesWhenNodeJoins verifies that pending tasks
// are promoted to ready when a matching node is registered via the callback.
func TestTriggerPendingTasks_PromotesWhenNodeJoins(t *testing.T) {
	tc := NewTriggerTestCluster(60 * time.Second)

	// Step 1: Create a task and manually set it to pending status
	_, err := tc.TaskStore.Create("task_trigger_001", "GPU task", []string{"gpu"})
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}
	// Force task to pending status (simulates a task waiting for requirements)
	tc.TaskStore.SetStatus("task_trigger_001", scheduler.TaskPending)

	// Verify task is pending
	pending, _ := tc.TaskStore.Get("task_trigger_001")
	if pending.Status != scheduler.TaskPending {
		t.Fatalf("expected task status 'pending', got '%s'", pending.Status)
	}

	// Step 2: Register a node with "gpu" capability (triggers onNodeOnline callback)
	tc.Registry.Register(&cluster.Node{
		ID:           "node_gpu",
		Name:         "GPU Node",
		Capabilities: []string{"gpu"},
	})

	// Wait briefly for the async callback to fire
	time.Sleep(100 * time.Millisecond)

	// Step 3: Verify the task was promoted to assigned
	updated, ok := tc.TaskStore.Get("task_trigger_001")
	if !ok {
		t.Fatal("task not found")
	}
	if updated.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task status 'assigned', got '%s'", updated.Status)
	}
	if updated.AssignedTo != "node_gpu" {
		t.Fatalf("expected assigned to 'node_gpu', got '%s'", updated.AssignedTo)
	}

	// Verify lease was created
	l, ok := tc.LeaseMgr.GetActiveForTask("task_trigger_001")
	if !ok {
		t.Fatal("expected active lease for task_trigger_001")
	}
	if l.NodeID != "node_gpu" {
		t.Errorf("lease node %s != expected node_gpu", l.NodeID)
	}
}

// TestTriggerPendingTasks_StaysPendingWhenNoMatch verifies that pending tasks
// remain pending when no matching node is registered.
func TestTriggerPendingTasks_StaysPendingWhenNoMatch(t *testing.T) {
	tc := NewTriggerTestCluster(60 * time.Second)

	// Create a task requiring "gpu" and force to pending
	tc.TaskStore.Create("task_trigger_002", "GPU task", []string{"gpu"})
	tc.TaskStore.SetStatus("task_trigger_002", scheduler.TaskPending)

	// Register a node WITHOUT "gpu" capability
	tc.Registry.Register(&cluster.Node{
		ID:           "node_coding",
		Name:         "Coding Node",
		Capabilities: []string{"coding"},
	})

	// Wait for async callback
	time.Sleep(100 * time.Millisecond)

	// Manually trigger (to test the function directly)
	promoted := tc.Scheduler.TriggerPendingTasks()
	if len(promoted) != 0 {
		t.Fatalf("expected no tasks promoted, got %d", len(promoted))
	}

	// Task should still be pending
	updated, ok := tc.TaskStore.Get("task_trigger_002")
	if !ok {
		t.Fatal("task not found")
	}
	if updated.Status != scheduler.TaskPending {
		t.Fatalf("expected task status 'pending', got '%s'", updated.Status)
	}
}

// TestTriggerPendingTasks_MultipleTasks verifies that multiple pending tasks
// can be promoted when matching nodes appear.
func TestTriggerPendingTasks_MultipleTasks(t *testing.T) {
	tc := NewTriggerTestCluster(60 * time.Second)

	// Create tasks and force them to pending
	tc.TaskStore.Create("task_multi_001", "GPU task", []string{"gpu"})
	tc.TaskStore.Create("task_multi_002", "Coding task", []string{"coding"})
	tc.TaskStore.Create("task_multi_003", "Browser task", []string{"browser"})
	tc.TaskStore.SetStatus("task_multi_001", scheduler.TaskPending)
	tc.TaskStore.SetStatus("task_multi_002", scheduler.TaskPending)
	tc.TaskStore.SetStatus("task_multi_003", scheduler.TaskPending)

	// All should be pending
	for _, id := range []string{"task_multi_001", "task_multi_002", "task_multi_003"} {
		task, _ := tc.TaskStore.Get(id)
		if task.Status != scheduler.TaskPending {
			t.Fatalf("task %s: expected pending, got %s", id, task.Status)
		}
	}

	// Register nodes that match all tasks
	tc.Registry.Register(&cluster.Node{
		ID:           "node_a",
		Name:         "Node A",
		Capabilities: []string{"gpu", "coding"},
	})
	tc.Registry.Register(&cluster.Node{
		ID:           "node_b",
		Name:         "Node B",
		Capabilities: []string{"browser"},
	})

	// Wait for async callbacks
	time.Sleep(100 * time.Millisecond)

	// All tasks should now be assigned
	for _, id := range []string{"task_multi_001", "task_multi_002", "task_multi_003"} {
		task, ok := tc.TaskStore.Get(id)
		if !ok {
			t.Fatalf("task %s not found", id)
		}
		if task.Status != scheduler.TaskAssigned {
			t.Errorf("task %s: expected assigned, got %s", id, task.Status)
		}
	}
}

// TestTriggerAPI_ManualTrigger verifies the POST /api/v1/schedule/trigger endpoint.
func TestTriggerAPI_ManualTrigger(t *testing.T) {
	tc := NewTriggerTestCluster(60 * time.Second)

	// Register a matching node FIRST (before any tasks exist, so the
	// onNodeOnline callback finds nothing to promote).
	tc.Registry.Register(&cluster.Node{
		ID:           "node_api",
		Name:         "API Node",
		Capabilities: []string{"gpu"},
	})

	// Wait for the async onNodeOnline goroutine to complete — without this,
	// the goroutine can race with task creation and promote/schedule the
	// task before the API trigger call.
	time.Sleep(200 * time.Millisecond)

	// Create a task and force to pending
	tc.TaskStore.Create("task_api_001", "GPU task", []string{"gpu"})
	tc.TaskStore.SetStatus("task_api_001", scheduler.TaskPending)

	// Call the trigger API
	body, _ := json.Marshal(map[string]interface{}{})
	req := httptest.NewRequest("POST", "/api/v1/schedule/trigger", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	tc.Server.Router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Check promoted tasks
	promoted, ok := resp["promoted"].([]interface{})
	if !ok {
		t.Fatal("expected 'promoted' in response")
	}
	if len(promoted) != 1 {
		t.Fatalf("expected 1 promoted task, got %d", len(promoted))
	}

	// Check scheduled tasks
	scheduled, ok := resp["scheduled"].([]interface{})
	if !ok {
		t.Fatal("expected 'scheduled' in response")
	}
	if len(scheduled) != 1 {
		t.Fatalf("expected 1 scheduled task, got %d", len(scheduled))
	}
}

// TestTriggerEndToEnd_NodeJoinTriggersSchedule verifies the full end-to-end flow:
// create pending task -> register node -> task gets scheduled automatically.
func TestTriggerEndToEnd_NodeJoinTriggersSchedule(t *testing.T) {
	tc := NewTriggerTestCluster(60 * time.Second)

	// Step 1: Create a task and force to pending
	_, err := tc.TaskStore.Create("task_e2e_001", "Research task", []string{"research"})
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}
	tc.TaskStore.SetStatus("task_e2e_001", scheduler.TaskPending)

	// Verify task is pending
	updated, _ := tc.TaskStore.Get("task_e2e_001")
	if updated.Status != scheduler.TaskPending {
		t.Fatalf("expected pending, got %s", updated.Status)
	}

	// Step 2: Register a node with "research" capability
	tc.Registry.Register(&cluster.Node{
		ID:           "node_research",
		Name:         "Research Node",
		Capabilities: []string{"research"},
	})

	// Wait for async callback
	time.Sleep(100 * time.Millisecond)

	// Step 3: Verify task was automatically assigned
	var ok bool
	updated, ok = tc.TaskStore.Get("task_e2e_001")
	if !ok {
		t.Fatal("task not found")
	}
	if updated.Status != scheduler.TaskAssigned {
		t.Fatalf("expected assigned, got %s", updated.Status)
	}
	if updated.AssignedTo != "node_research" {
		t.Fatalf("expected assigned to node_research, got %s", updated.AssignedTo)
	}

	// Step 4: Create another pending task requiring "coding"
	tc.TaskStore.Create("task_e2e_002", "Coding task", []string{"coding"})
	tc.TaskStore.SetStatus("task_e2e_002", scheduler.TaskPending)

	// It should be pending (no coding node yet)
	pending, _ := tc.TaskStore.Get("task_e2e_002")
	if pending.Status != scheduler.TaskPending {
		t.Fatalf("expected task_e2e_002 pending, got %s", pending.Status)
	}

	// Step 5: Register a node with "coding" capability
	tc.Registry.Register(&cluster.Node{
		ID:           "node_coding",
		Name:         "Coding Node",
		Capabilities: []string{"coding"},
	})

	// Wait for async callback
	time.Sleep(100 * time.Millisecond)

	// Step 6: Verify task_e2e_002 was automatically assigned
	updated2, ok := tc.TaskStore.Get("task_e2e_002")
	if !ok {
		t.Fatal("task_e2e_002 not found")
	}
	if updated2.Status != scheduler.TaskAssigned {
		t.Fatalf("expected assigned, got %s", updated2.Status)
	}
	if updated2.AssignedTo != "node_coding" {
		t.Fatalf("expected assigned to node_coding, got %s", updated2.AssignedTo)
	}
}

// TestTriggerRecoveryWithPendingTasks verifies that recovery works correctly
// with the trigger mechanism.
func TestTriggerRecoveryWithPendingTasks(t *testing.T) {
	tc := NewTriggerTestCluster(10 * time.Second)

	// Register two nodes
	tc.Registry.Register(&cluster.Node{
		ID:           "node_a",
		Name:         "Node A",
		Capabilities: []string{"coding"},
	})
	tc.Registry.Register(&cluster.Node{
		ID:           "node_b",
		Name:         "Node B",
		Capabilities: []string{"coding"},
	})

	// Create a task and force to pending, then trigger+schedule
	tc.TaskStore.Create("task_recovery_trigger_001", "Coding task", []string{"coding"})
	tc.TaskStore.SetStatus("task_recovery_trigger_001", scheduler.TaskPending)

	// Trigger and schedule
	promoted := tc.Scheduler.TriggerPendingTasks()
	if len(promoted) != 1 {
		t.Fatalf("expected 1 promoted task, got %d", len(promoted))
	}
	scheduled := tc.Scheduler.SchedulePending()
	if len(scheduled) != 1 {
		t.Fatalf("expected 1 scheduled task, got %d", len(scheduled))
	}

	// Get the assigned node
	assignedNode := scheduled[0][1]
	fmt.Printf("Task assigned to: %s\n", assignedNode)

	// Simulate node failure: mark offline, revoke lease, unassign task
	tc.Registry.UpdateStatus(assignedNode, cluster.NodeOffline)
	if l, ok := tc.LeaseMgr.GetActiveForTask("task_recovery_trigger_001"); ok {
		tc.LeaseMgr.Revoke(l.ID)
	}
	tc.TaskStore.Unassign("task_recovery_trigger_001")

	// Trigger pending (the task was unassigned to ready, not pending)
	_ = tc.Scheduler.TriggerPendingTasks()
	// SchedulePending picks up the ready task and assigns it to the remaining online node
	scheduled2 := tc.Scheduler.SchedulePending()
	if len(scheduled2) != 1 {
		t.Fatalf("expected 1 rescheduled task, got %d", len(scheduled2))
	}

	// Verify it was assigned to a different node
	updated, ok := tc.TaskStore.Get("task_recovery_trigger_001")
	if !ok {
		t.Fatal("task not found")
	}
	if updated.AssignedTo == assignedNode {
		t.Fatalf("task still assigned to failed node %s", updated.AssignedTo)
	}
	if updated.Status != scheduler.TaskAssigned {
		t.Fatalf("expected assigned, got %s", updated.Status)
	}
}

// TestTriggerManualExecution verifies manual trigger and schedule execution.
func TestTriggerManualExecution(t *testing.T) {
	tc := NewTriggerTestCluster(60 * time.Second)

	// Create tasks and force to pending
	tc.TaskStore.Create("task_manual_001", "GPU task", []string{"gpu"})
	tc.TaskStore.Create("task_manual_002", "Browser task", []string{"browser"})
	tc.TaskStore.SetStatus("task_manual_001", scheduler.TaskPending)
	tc.TaskStore.SetStatus("task_manual_002", scheduler.TaskPending)

	// Register a node with GPU only
	tc.Registry.Register(&cluster.Node{
		ID:           "node_gpu",
		Name:         "GPU Node",
		Capabilities: []string{"gpu"},
	})

	// Manually trigger and schedule
	promoted := tc.Scheduler.TriggerPendingTasks()
	scheduled := tc.Scheduler.SchedulePending()

	// Only the GPU task should be promoted and scheduled
	if len(promoted) != 1 {
		t.Fatalf("expected 1 promoted task, got %d", len(promoted))
	}
	if promoted[0] != "task_manual_001" {
		t.Fatalf("expected task_manual_001 promoted, got %s", promoted[0])
	}
	if len(scheduled) != 1 {
		t.Fatalf("expected 1 scheduled task, got %d", len(scheduled))
	}

	// Browser task should still be pending
	browserTask, _ := tc.TaskStore.Get("task_manual_002")
	if browserTask.Status != scheduler.TaskPending {
		t.Fatalf("expected browser task pending, got %s", browserTask.Status)
	}
}
