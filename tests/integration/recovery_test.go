package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/recovery"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

// RecoveryTestCluster is a simplified cluster for recovery testing
// (no watchdog — we trigger offline events directly).
type RecoveryTestCluster struct {
	Registry    *cluster.Registry
	Scheduler   *scheduler.Scheduler
	LeaseMgr    *lease.Manager
	TaskStore   *scheduler.TaskStore
	Recovery    *recovery.Detector
	Revoker     *recovery.Revoker
	Rescheduler *recovery.Rescheduler
	RecLog      *recovery.Log
}

func NewRecoveryTestCluster(leaseTTL time.Duration) *RecoveryTestCluster {
	registry := cluster.NewRegistry()
	taskStore := scheduler.NewTaskStore()
	leaseMgr := lease.NewManager()
	recLog := recovery.NewLog()
	sched := scheduler.NewScheduler(registry, taskStore, leaseMgr, leaseTTL)
	revoker := recovery.NewRevoker(leaseMgr, recLog)
	resched := recovery.NewRescheduler(sched, recLog)
	detector := recovery.NewDetector(revoker, resched, leaseMgr, recLog)

	return &RecoveryTestCluster{
		Registry:    registry,
		Scheduler:   sched,
		LeaseMgr:    leaseMgr,
		TaskStore:   taskStore,
		Recovery:    detector,
		Revoker:     revoker,
		Rescheduler: resched,
		RecLog:      recLog,
	}
}

func (tc *RecoveryTestCluster) Start() {
	tc.Recovery.Start()
}

func (tc *RecoveryTestCluster) Stop() {
	tc.Recovery.Stop()
}

func (tc *RecoveryTestCluster) RegisterNode(id, name string, caps []string) {
	tc.Registry.Register(&cluster.Node{
		ID:           id,
		Name:         name,
		Capabilities: caps,
	})
}

func (tc *RecoveryTestCluster) SubmitTask(id, title string, requires []string) *scheduler.Task {
	if _, err := tc.TaskStore.Create(id, title, requires); err != nil {
		panic(fmt.Sprintf("SubmitTask: %v", err))
	}
	tc.Scheduler.SchedulePending()
	t, _ := tc.TaskStore.Get(id)
	return t
}

// SimulateNodeFailure marks a node offline and triggers recovery.
func (tc *RecoveryTestCluster) SimulateNodeFailure(nodeID string) {
	// Mark node as offline in registry (so SchedulePending won't pick it)
	tc.Registry.UpdateStatus(nodeID, cluster.NodeOffline)
	// Trigger recovery
	tc.Recovery.NotifyOffline(nodeID)
}

// TestScenario2_HeartbeatRecovery verifies fault detection and recovery:
// register nodes -> assign task -> trigger offline -> verify recovery
func TestScenario2_HeartbeatRecovery(t *testing.T) {
	tc := NewRecoveryTestCluster(10 * time.Second)
	defer tc.Stop()
	tc.Start()

	// Step 1: Register 3 nodes
	tc.RegisterNode("node_a", "Node-A", []string{"coding", "gpu"})
	tc.RegisterNode("node_b", "Node-B", []string{"coding", "research"})
	tc.RegisterNode("node_c", "Node-C", []string{"gpu", "browser"})

	// Step 2: Submit a task requiring "coding" (both node_a and node_b qualify)
	task := tc.SubmitTask("task_recovery_001", "Recovery test task", []string{"coding"})
	if task.Status != "assigned" {
		t.Fatalf("expected task status 'assigned', got '%s'", task.Status)
	}
	originalNode := task.AssignedTo
	t.Logf("Task assigned to: %s", originalNode)

	// Step 3: Verify lease exists
	originalLease, ok := tc.LeaseMgr.GetActiveForTask("task_recovery_001")
	if !ok {
		t.Fatal("expected active lease for task_recovery_001")
	}

	// Step 4: Simulate node failure
	tc.SimulateNodeFailure(originalNode)

	// Step 5: Wait for recovery to process (async via channel)
	time.Sleep(500 * time.Millisecond)

	// Step 6: Verify the ORIGINAL lease was revoked
	originalLeaseAfter, ok := tc.LeaseMgr.Get(originalLease.ID)
	if !ok {
		t.Fatal("original lease not found")
	}
	if originalLeaseAfter.Status != lease.LeaseRevoked {
		t.Errorf("expected original lease to be revoked, got '%s'", originalLeaseAfter.Status)
	}

	// Step 7: Verify the task was rescheduled to a different node
	updatedTask, ok := tc.TaskStore.Get("task_recovery_001")
	if !ok {
		t.Fatal("task not found after recovery")
	}
	if updatedTask.Status != "assigned" {
		t.Errorf("expected task to be reassigned, status = %s", updatedTask.Status)
	}
	if updatedTask.AssignedTo == originalNode {
		t.Errorf("task still assigned to failed node %s", updatedTask.AssignedTo)
	}
	t.Logf("Task rescheduled to: %s", updatedTask.AssignedTo)

	// Step 8: Verify a NEW active lease exists for the new node
	newLease, ok := tc.LeaseMgr.GetActiveForTask("task_recovery_001")
	if !ok {
		t.Fatal("expected new active lease after rescheduling")
	}
	if newLease.NodeID != updatedTask.AssignedTo {
		t.Errorf("new lease holder %s != assigned node %s", newLease.NodeID, updatedTask.AssignedTo)
	}

	// Step 9: Verify recovery log was recorded
	events := tc.RecLog.GetEvents()
	if len(events) == 0 {
		t.Error("expected recovery events in log")
	}

	// Verify at least one revoke_lease event
	foundRevoke := false
	for _, e := range events {
		if e.Action == "revoke_lease" && e.NodeID == originalNode {
			foundRevoke = true
			break
		}
	}
	if !foundRevoke {
		t.Error("expected revoke_lease event for the failed node")
	}

	// Step 10: Check recovery stats
	stats := tc.RecLog.Stats()
	if stats["total"] == 0 {
		t.Error("expected non-zero recovery stats")
	}
	t.Logf("Recovery stats: %+v", stats)
}

// TestScenario2b_MultipleTaskRecovery verifies recovery when a node holds multiple tasks.
func TestScenario2b_MultipleTaskRecovery(t *testing.T) {
	tc := NewRecoveryTestCluster(10 * time.Second)
	defer tc.Stop()
	tc.Start()

	tc.RegisterNode("node_a", "Node-A", []string{"coding"})
	tc.RegisterNode("node_b", "Node-B", []string{"coding"})

	// Submit two tasks — both will be assigned (possibly to same node)
	task1 := tc.SubmitTask("task_multi_001", "Task 1", []string{"coding"})
	task2 := tc.SubmitTask("task_multi_002", "Task 2", []string{"coding"})

	t.Logf("Task 1 assigned to: %s", task1.AssignedTo)
	t.Logf("Task 2 assigned to: %s", task2.AssignedTo)

	// Find which node has tasks and simulate its failure
	var nodeWithTasks string
	if task1.AssignedTo == task2.AssignedTo {
		nodeWithTasks = task1.AssignedTo
	} else {
		// Tasks split — fail node_a which has task1
		nodeWithTasks = task1.AssignedTo
	}

	// Trigger offline for the node with tasks (also marks it offline in registry)
	tc.SimulateNodeFailure(nodeWithTasks)
	time.Sleep(500 * time.Millisecond)

	// Verify tasks were rescheduled to a different node
	for _, task := range []*scheduler.Task{task1, task2} {
		updated, ok := tc.TaskStore.Get(task.ID)
		if !ok {
			t.Errorf("task %s not found", task.ID)
			continue
		}
		t.Logf("Task %s status: %s, assigned: %s", task.ID, updated.Status, updated.AssignedTo)
		if updated.Status == "assigned" && updated.AssignedTo == nodeWithTasks {
			t.Errorf("task %s still assigned to failed node", task.ID)
		}
	}
}
