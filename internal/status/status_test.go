package status

import (
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

func setupStatusView(t *testing.T) *StatusView {
	t.Helper()
	registry := cluster.NewRegistry()
	taskStore := scheduler.NewTaskStore()
	leaseMgr := lease.NewManager()
	return NewStatusView(registry, taskStore, leaseMgr)
}

func TestQuery_EmptyCluster(t *testing.T) {
	sv := setupStatusView(t)
	entries, summary := sv.Query(Filter{})

	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
	if summary.TotalNodes != 0 {
		t.Errorf("expected 0 total nodes, got %d", summary.TotalNodes)
	}
	if summary.TotalTasks != 0 {
		t.Errorf("expected 0 total tasks, got %d", summary.TotalTasks)
	}
	if summary.ActiveLeases != 0 {
		t.Errorf("expected 0 active leases, got %d", summary.ActiveLeases)
	}
}

func TestQuery_WithNodes(t *testing.T) {
	sv := setupStatusView(t)

	sv.Registry.Register(&cluster.Node{
		ID:           "node_1",
		Name:         "worker-1",
		Capabilities: []string{"gpu", "cuda"},
	})

	sv.Registry.Register(&cluster.Node{
		ID:           "node_2",
		Name:         "worker-2",
		Capabilities: []string{"cpu"},
	})

	entries, summary := sv.Query(Filter{})

	if len(entries) != 0 {
		t.Errorf("expected 0 task entries (no tasks yet), got %d", len(entries))
	}
	if summary.TotalNodes != 2 {
		t.Errorf("expected 2 total nodes, got %d", summary.TotalNodes)
	}
	if summary.OnlineNodes != 2 {
		t.Errorf("expected 2 online nodes, got %d", summary.OnlineNodes)
	}
}

func TestQuery_WithTaskAssigned(t *testing.T) {
	sv := setupStatusView(t)

	sv.Registry.Register(&cluster.Node{
		ID:           "node_1",
		Name:         "worker-1",
		Capabilities: []string{"gpu"},
	})

	// Create a task and assign it to node_1
	taskID := "task_abc"
	sv.TaskStore.Create(taskID, "test task", []string{"gpu"})
	sv.TaskStore.SetAssigned(taskID, "node_1")

	// Create a lease for the task
	sv.LeaseMgr.Create(taskID, "node_1", 5*time.Minute)

	entries, summary := sv.Query(Filter{})

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.TaskID != taskID {
		t.Errorf("expected task_id %q, got %q", taskID, entry.TaskID)
	}
	if entry.NodeID != "node_1" {
		t.Errorf("expected node_id 'node_1', got %q", entry.NodeID)
	}
	if entry.NodeName != "worker-1" {
		t.Errorf("expected node_name 'worker-1', got %q", entry.NodeName)
	}
	if entry.Capabilities[0] != "gpu" {
		t.Errorf("expected capability 'gpu', got %q", entry.Capabilities[0])
	}
	if entry.LeaseStatus != "active" {
		t.Errorf("expected lease_status 'active', got %q", entry.LeaseStatus)
	}
	if summary.TotalTasks != 1 {
		t.Errorf("expected 1 total task, got %d", summary.TotalTasks)
	}
	if summary.ActiveLeases != 1 {
		t.Errorf("expected 1 active lease, got %d", summary.ActiveLeases)
	}
}

func TestQuery_FilterByStatus(t *testing.T) {
	sv := setupStatusView(t)

	sv.TaskStore.Create("task_1", "task 1", nil)
	sv.TaskStore.Create("task_2", "task 2", nil)
	// task_2 will be "pending" (default), let's complete task_1
	sv.TaskStore.SetStatus("task_1", scheduler.TaskCompleted)

	// Filter by "completed" — should return 1
	entries, _ := sv.Query(Filter{Status: "completed"})
	if len(entries) != 1 {
		t.Errorf("expected 1 completed entry, got %d", len(entries))
	}
	if entries[0].TaskID != "task_1" {
		t.Errorf("expected task_id 'task_1', got %q", entries[0].TaskID)
	}

	// Filter by "pending" — should return 0 (tasks are created as "ready")
	entries, _ = sv.Query(Filter{Status: "pending"})
	if len(entries) != 0 {
		t.Errorf("expected 0 pending entries, got %d", len(entries))
	}

	// Filter by "ready" — should return 1 (task_2 is still ready)
	entries, _ = sv.Query(Filter{Status: "ready"})
	if len(entries) != 1 {
		t.Errorf("expected 1 ready entry, got %d", len(entries))
	}
}

func TestQuery_FilterByNode(t *testing.T) {
	sv := setupStatusView(t)

	sv.Registry.Register(&cluster.Node{
		ID:           "node_1",
		Name:         "worker-1",
		Capabilities: []string{"gpu"},
	})
	sv.Registry.Register(&cluster.Node{
		ID:           "node_2",
		Name:         "worker-2",
		Capabilities: []string{"cpu"},
	})

	sv.TaskStore.Create("task_1", "task for node 1", nil)
	sv.TaskStore.SetAssigned("task_1", "node_1")

	sv.TaskStore.Create("task_2", "task for node 2", nil)
	sv.TaskStore.SetAssigned("task_2", "node_2")

	// Filter by node_1 — should return 1
	entries, _ := sv.Query(Filter{NodeID: "node_1"})
	if len(entries) != 1 {
		t.Errorf("expected 1 entry for node_1, got %d", len(entries))
	}
	if entries[0].NodeID != "node_1" {
		t.Errorf("expected node_id 'node_1', got %q", entries[0].NodeID)
	}

	// Filter by node_2 — should return 1
	entries, _ = sv.Query(Filter{NodeID: "node_2"})
	if len(entries) != 1 {
		t.Errorf("expected 1 entry for node_2, got %d", len(entries))
	}

	// Filter by nonexistent node — should return 0
	entries, _ = sv.Query(Filter{NodeID: "node_999"})
	if len(entries) != 0 {
		t.Errorf("expected 0 entries for nonexistent node, got %d", len(entries))
	}
}

func TestQuery_FilterByCapability(t *testing.T) {
	sv := setupStatusView(t)

	sv.Registry.Register(&cluster.Node{
		ID:           "node_1",
		Name:         "gpu-worker",
		Capabilities: []string{"gpu", "cuda"},
	})
	sv.Registry.Register(&cluster.Node{
		ID:           "node_2",
		Name:         "cpu-worker",
		Capabilities: []string{"cpu"},
	})

	sv.TaskStore.Create("task_1", "gpu task", nil)
	sv.TaskStore.SetAssigned("task_1", "node_1")

	sv.TaskStore.Create("task_2", "cpu task", nil)
	sv.TaskStore.SetAssigned("task_2", "node_2")

	// Filter by "gpu" — should return 1 (only node_1 has gpu)
	entries, _ := sv.Query(Filter{Capability: "gpu"})
	if len(entries) != 1 {
		t.Errorf("expected 1 entry for capability=gpu, got %d", len(entries))
	}
	if entries[0].NodeID != "node_1" {
		t.Errorf("expected node_id 'node_1', got %q", entries[0].NodeID)
	}

	// Filter by "cuda" — should return 1 (node_1 has cuda)
	entries, _ = sv.Query(Filter{Capability: "cuda"})
	if len(entries) != 1 {
		t.Errorf("expected 1 entry for capability=cuda, got %d", len(entries))
	}

	// Filter by "cpu" — should return 1 (only node_2 has cpu)
	entries, _ = sv.Query(Filter{Capability: "cpu"})
	if len(entries) != 1 {
		t.Errorf("expected 1 entry for capability=cpu, got %d", len(entries))
	}
	if entries[0].NodeID != "node_2" {
		t.Errorf("expected node_id 'node_2', got %q", entries[0].NodeID)
	}

	// Filter by "tpu" — should return 0 (no node has tpu)
	entries, _ = sv.Query(Filter{Capability: "tpu"})
	if len(entries) != 0 {
		t.Errorf("expected 0 entries for capability=tpu, got %d", len(entries))
	}
}

func TestQuery_SummaryCounts(t *testing.T) {
	sv := setupStatusView(t)

	sv.Registry.Register(&cluster.Node{ID: "node_1", Name: "w1", Status: cluster.NodeOnline})
	sv.Registry.Register(&cluster.Node{ID: "node_2", Name: "w2", Status: cluster.NodeOnline})
	sv.Registry.UpdateStatus("node_2", cluster.NodeOffline)

	sv.TaskStore.Create("task_1", "t1", nil)
	sv.TaskStore.Create("task_2", "t2", nil)
	sv.TaskStore.Create("task_3", "t3", nil)
	sv.TaskStore.SetStatus("task_1", scheduler.TaskCompleted)
	sv.TaskStore.SetStatus("task_2", scheduler.TaskFailed)

	sv.LeaseMgr.Create("task_3", "node_1", 5*time.Minute)

	_, summary := sv.Query(Filter{})

	if summary.TotalNodes != 2 {
		t.Errorf("expected 2 total nodes, got %d", summary.TotalNodes)
	}
	if summary.OnlineNodes != 1 {
		t.Errorf("expected 1 online node, got %d", summary.OnlineNodes)
	}
	if summary.TotalTasks != 3 {
		t.Errorf("expected 3 total tasks, got %d", summary.TotalTasks)
	}
	if summary.TasksByStatus["completed"] != 1 {
		t.Errorf("expected 1 completed task, got %d", summary.TasksByStatus["completed"])
	}
	if summary.TasksByStatus["failed"] != 1 {
		t.Errorf("expected 1 failed task, got %d", summary.TasksByStatus["failed"])
	}
	if summary.ActiveLeases != 1 {
		t.Errorf("expected 1 active lease, got %d", summary.ActiveLeases)
	}
}

func TestQuery_SortOrder(t *testing.T) {
	sv := setupStatusView(t)

	sv.TaskStore.Create("task_c", "third task", nil)
	sv.TaskStore.Create("task_a", "first task", nil)
	sv.TaskStore.Create("task_b", "second task", nil)

	entries, _ := sv.Query(Filter{})

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// All pending — sorted by task_id
	if entries[0].TaskID != "task_a" {
		t.Errorf("expected first entry task_a, got %q", entries[0].TaskID)
	}
	if entries[1].TaskID != "task_b" {
		t.Errorf("expected second entry task_b, got %q", entries[1].TaskID)
	}
	if entries[2].TaskID != "task_c" {
		t.Errorf("expected third entry task_c, got %q", entries[2].TaskID)
	}
}
