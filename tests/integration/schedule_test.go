package integration

import (
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

// TestScenario1_NormalScheduling verifies the full scheduling flow:
// cluster init -> register nodes -> submit task -> validate assignment
func TestScenario1_NormalScheduling(t *testing.T) {
	cluster := NewTestCluster(60 * time.Second)
	defer cluster.Stop()
	cluster.Start()

	// Step 1: Register 3 nodes with different capabilities
	cluster.RegisterNode("node_a", "Node-A", []string{"coding", "gpu"})
	cluster.RegisterNode("node_b", "Node-B", []string{"coding", "research"})
	cluster.RegisterNode("node_c", "Node-C", []string{"gpu", "browser"})

	// Verify all nodes registered
	if cluster.Registry.Count() != 3 {
		t.Fatalf("expected 3 nodes, got %d", cluster.Registry.Count())
	}

	// Verify all nodes are online
	if cluster.Registry.OnlineCount() != 3 {
		t.Fatalf("expected 3 online nodes, got %d", cluster.Registry.OnlineCount())
	}

	// Step 2: Submit a task requiring "gpu" capability
	task := cluster.SubmitTask("task_001", "GPU inference job", []string{"gpu"})
	if task.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task status 'assigned', got '%s'", task.Status)
	}

	// Step 3: Verify task was assigned to a node with GPU capability
	assignedNode, ok := cluster.Registry.Get(task.AssignedTo)
	if !ok {
		t.Fatalf("assigned node %s not found in registry", task.AssignedTo)
	}

	// Check that the assigned node has the "gpu" capability
	hasGPU := false
	for _, cap := range assignedNode.Capabilities {
		if cap == "gpu" {
			hasGPU = true
			break
		}
	}
	if !hasGPU {
		t.Errorf("assigned node %s does not have 'gpu' capability", task.AssignedTo)
	}

	// Step 4: Verify a lease was created for this task
	lease, ok := cluster.LeaseMgr.GetActiveForTask("task_001")
	if !ok {
		t.Fatal("expected active lease for task_001")
	}
	if lease.NodeID != task.AssignedTo {
		t.Errorf("lease node %s != assigned node %s", lease.NodeID, task.AssignedTo)
	}

	// Step 5: Submit another task with no requirements (any node should match)
	task2 := cluster.SubmitTask("task_002", "Generic task", nil)
	if task2.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task2 status 'assigned', got '%s'", task2.Status)
	}

	// Step 6: Submit a task requiring "coding" + "gpu" (only node_a qualifies)
	task3 := cluster.SubmitTask("task_003", "Coding + GPU task", []string{"coding", "gpu"})
	if task3.Status != scheduler.TaskAssigned {
		t.Fatalf("expected task3 status 'assigned', got '%s'", task3.Status)
	}
	if task3.AssignedTo != "node_a" {
		t.Errorf("expected task3 assigned to node_a, got %s", task3.AssignedTo)
	}

	// Step 7: Verify node states are correct
	for _, id := range []string{"node_a", "node_b", "node_c"} {
		n, ok := cluster.Registry.Get(id)
		if !ok {
			t.Errorf("node %s not found", id)
			continue
		}
		if n.Status != "online" {
			t.Errorf("node %s status = %s, want online", id, n.Status)
		}
	}
}
