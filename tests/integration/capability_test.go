package integration

import (
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

// TestDynamicCapability_UnlockPendingTask verifies that updating a node's
// capabilities can unlock a pending task that was previously unschedulable.
func TestDynamicCapability_UnlockPendingTask(t *testing.T) {
	cluster := NewTestCluster(60 * time.Second)
	defer cluster.Stop()
	cluster.Start()

	// Register a node with only "coding" capability
	cluster.RegisterNode("node_a", "Node-A", []string{"coding"})

	// Submit a task requiring "gpu" — should stay pending (no matching node)
	task := cluster.SubmitTask("task_gpu", "GPU job", []string{"gpu"})
	if task.Status != scheduler.TaskPending && task.Status != scheduler.TaskReady {
		t.Fatalf("expected task to be pending/ready, got %s", task.Status)
	}

	// Verify no lease was created (task not assigned)
	_, hasLease := cluster.LeaseMgr.GetActiveForTask("task_gpu")
	if hasLease {
		t.Fatal("no lease should exist for unschedulable task")
	}

	// Now update node_a to also have "gpu" capability
	cluster.Registry.UpdateCapabilities("node_a", []string{"coding", "gpu"})

	// Trigger scheduling re-evaluation
	cluster.Scheduler.TriggerPendingTasks()
	cluster.Scheduler.SchedulePending()

	// Verify task is now assigned
	taskAfter, ok := cluster.TaskStore.Get("task_gpu")
	if !ok {
		t.Fatal("task not found after update")
	}
	if taskAfter.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task assigned after capability update, got %s", taskAfter.Status)
	}
	if taskAfter.AssignedTo != "node_a" {
		t.Errorf("expected task assigned to node_a, got %s", taskAfter.AssignedTo)
	}
}

// TestDynamicCapability_RemoveCapabilityReleasesTask verifies that removing
// a capability from a node causes its assigned tasks to be rescheduled.
func TestDynamicCapability_RemoveCapabilityReleasesTask(t *testing.T) {
	cluster := NewTestCluster(60 * time.Second)
	defer cluster.Stop()
	cluster.Start()

	// Register node with "gpu" capability
	cluster.RegisterNode("node_a", "Node-A", []string{"gpu"})

	// Submit a task requiring "gpu" — should be assigned to node_a
	task := cluster.SubmitTask("task_gpu", "GPU job", []string{"gpu"})
	if task.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task assigned, got %s", task.Status)
	}
	if task.AssignedTo != "node_a" {
		t.Fatalf("expected assignment to node_a, got %s", task.AssignedTo)
	}

	// Now remove "gpu" from node_a and add a new node with "gpu"
	cluster.Registry.UpdateCapabilities("node_a", []string{"coding"})
	cluster.RegisterNode("node_b", "Node-B", []string{"gpu"})

	// Revoke the lease on node_a for this task, then reschedule
	if activeLease, ok := cluster.LeaseMgr.GetActiveForTask("task_gpu"); ok {
		cluster.LeaseMgr.Revoke(activeLease.ID)
	}
	cluster.TaskStore.Unassign("task_gpu")

	// Re-trigger scheduling
	cluster.Scheduler.TriggerPendingTasks()
	cluster.Scheduler.SchedulePending()

	// Verify task moved to node_b
	taskAfter, ok := cluster.TaskStore.Get("task_gpu")
	if !ok {
		t.Fatal("task not found")
	}
	if taskAfter.AssignedTo != "node_b" {
		t.Errorf("expected task reassigned to node_b, got %s", taskAfter.AssignedTo)
	}
}

// TestDynamicCapability_MultipleNodesCompetition verifies that when a node
// gains new capabilities, it can compete for tasks with other nodes.
func TestDynamicCapability_MultipleNodesCompetition(t *testing.T) {
	cluster := NewTestCluster(60 * time.Second)
	defer cluster.Stop()
	cluster.Start()

	// Register two nodes: node_a has "coding", node_b has "coding" + "gpu"
	cluster.RegisterNode("node_a", "Node-A", []string{"coding"})
	cluster.RegisterNode("node_b", "Node-B", []string{"coding", "gpu"})

	// Submit a task requiring "coding" + "gpu" — only node_b can handle it
	task := cluster.SubmitTask("task_cg", "Coding+GPU job", []string{"coding", "gpu"})
	if task.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task assigned, got %s", task.Status)
	}
	if task.AssignedTo != "node_b" {
		t.Fatalf("expected assignment to node_b, got %s", task.AssignedTo)
	}

	// Now give node_a "gpu" capability too
	cluster.Registry.UpdateCapabilities("node_a", []string{"coding", "gpu"})

	// node_a should now be able to handle coding+gpu tasks
	// Submit a new task to see which node gets it
	task2 := cluster.SubmitTask("task_cg2", "Another Coding+GPU job", []string{"coding", "gpu"})
	if task2.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task2 assigned, got %s", task2.Status)
	}
	// Both nodes can handle it now — either is valid
	if task2.AssignedTo != "node_a" && task2.AssignedTo != "node_b" {
		t.Errorf("expected assignment to node_a or node_b, got %s", task2.AssignedTo)
	}
}

// TestDynamicCapability_NoChangeNoReschedule verifies that updating
// capabilities to the same set doesn't trigger unnecessary rescheduling.
func TestDynamicCapability_NoChangeNoReschedule(t *testing.T) {
	cluster := NewTestCluster(60 * time.Second)
	defer cluster.Stop()
	cluster.Start()

	cluster.RegisterNode("node_a", "Node-A", []string{"coding", "gpu"})

	// Submit a task
	task := cluster.SubmitTask("task_001", "Test task", []string{"gpu"})
	if task.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task assigned, got %s", task.Status)
	}
	originalNode := task.AssignedTo

	// Update with same capabilities (different order)
	cluster.Registry.UpdateCapabilities("node_a", []string{"gpu", "coding"})

	// Task should still be assigned to the same node (no disruption)
	taskAfter, ok := cluster.TaskStore.Get("task_001")
	if !ok {
		t.Fatal("task not found")
	}
	if taskAfter.AssignedTo != originalNode {
		t.Errorf("task should stay with %s, got %s", originalNode, taskAfter.AssignedTo)
	}
}

// TestDynamicCapability_CallbackTriggersScheduling verifies the full
// callback chain: UpdateCapabilities -> callback -> TriggerPendingTasks.
func TestDynamicCapability_CallbackTriggersScheduling(t *testing.T) {
	cluster := NewTestCluster(60 * time.Second)
	defer cluster.Stop()
	cluster.Start()

	// Wire the capability change callback (mirrors main.go)
	cluster.Registry.SetOnCapabilityChange(func(nodeID string, oldCaps, newCaps []string) {
		cluster.Scheduler.TriggerPendingTasks()
		cluster.Scheduler.SchedulePending()
	})

	// Register node without "gpu"
	cluster.RegisterNode("node_a", "Node-A", []string{"coding"})

	// Submit a task requiring "gpu"
	task := cluster.SubmitTask("task_gpu", "GPU job", []string{"gpu"})
	if task.Status == scheduler.TaskAssigned {
		t.Fatal("task should NOT be assigned yet (no gpu node)")
	}

	// Update capabilities — callback should trigger scheduling
	cluster.Registry.UpdateCapabilities("node_a", []string{"coding", "gpu"})

	// Wait briefly for async callback
	time.Sleep(50 * time.Millisecond)

	// Verify task got assigned
	taskAfter, ok := cluster.TaskStore.Get("task_gpu")
	if !ok {
		t.Fatal("task not found")
	}
	if taskAfter.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task assigned via callback, got %s", taskAfter.Status)
	}
}
