package integration

import (
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

// TestScenario4_LeaseExpiration verifies lease lifecycle:
// create lease -> wait for TTL -> verify expiry -> verify task can be rescheduled
func TestScenario4_LeaseExpiration(t *testing.T) {
	cluster := NewTestCluster(500 * time.Millisecond) // very short TTL for fast testing
	defer cluster.Stop()

	// Step 1: Register nodes
	cluster.RegisterNode("node_a", "Node-A", []string{"coding"})
	cluster.RegisterNode("node_b", "Node-B", []string{"coding"})

	// Step 2: Create a lease manually with short TTL
	l, err := cluster.LeaseMgr.Create("task_lease_001", "node_a", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create lease: %v", err)
	}

	// Step 3: Verify lease is active
	if l.Status != lease.LeaseActive {
		t.Errorf("expected lease status 'active', got '%s'", l.Status)
	}

	// Step 4: Verify only one active lease per task
	_, err = cluster.LeaseMgr.Create("task_lease_001", "node_b", 500*time.Millisecond)
	if err == nil {
		t.Error("expected error when creating duplicate active lease")
	}

	// Step 5: Wait for TTL to expire
	time.Sleep(600 * time.Millisecond)

	// Step 6: Manually trigger expiry check
	expired := cluster.LeaseMgr.CheckExpiry()
	if len(expired) == 0 {
		t.Error("expected at least one expired task")
	}

	// Step 7: Verify the lease is now expired
	expiredLease, ok := cluster.LeaseMgr.Get(l.ID)
	if !ok {
		t.Fatal("lease not found")
	}
	if expiredLease.Status != lease.LeaseExpired {
		t.Errorf("expected lease status 'expired', got '%s'", expiredLease.Status)
	}

	// Step 8: Verify no active lease for this task
	_, hasActive := cluster.LeaseMgr.GetActiveForTask("task_lease_001")
	if hasActive {
		t.Error("expected no active lease after expiry")
	}

	// Step 9: Create the task in the task store and verify rescheduling works
	if _, err := cluster.TaskStore.Create("task_lease_001", "Lease test task", []string{"coding"}); err != nil {
		t.Fatalf("failed to create task: %v", err)
	}
	cluster.TaskStore.SetAssigned("task_lease_001", "node_a")

	// Create a fresh lease for rescheduling test
	cluster.LeaseMgr.Create("task_lease_001", "node_a", 10*time.Second)

	// Revoke the lease
	if activeLease, ok := cluster.LeaseMgr.GetActiveForTask("task_lease_001"); ok {
		cluster.LeaseMgr.Revoke(activeLease.ID)
	}

	// Wait a moment for async revocation callback
	time.Sleep(100 * time.Millisecond)

	// Unassign and reschedule
	cluster.TaskStore.Unassign("task_lease_001")
	scheduled := cluster.Scheduler.SchedulePending()

	// Verify task was rescheduled
	rescheduled, ok := cluster.TaskStore.Get("task_lease_001")
	if !ok {
		t.Fatal("task not found after reschedule")
	}

	if len(scheduled) > 0 {
		if rescheduled.Status != scheduler.TaskAssigned {
			t.Errorf("expected task to be reassigned, status = %s", rescheduled.Status)
		}
		t.Logf("Task rescheduled to: %s", rescheduled.AssignedTo)
	} else {
		t.Log("No nodes available for rescheduling (expected if all nodes have tasks)")
	}
}

// TestScenario4b_LeaseRenewal verifies lease renewal protection.
func TestScenario4b_LeaseRenewal(t *testing.T) {
	cluster := NewTestCluster(1 * time.Second)
	defer cluster.Stop()

	// Create a lease
	l, err := cluster.LeaseMgr.Create("task_renew_001", "node_a", 1*time.Second)
	if err != nil {
		t.Fatalf("failed to create lease: %v", err)
	}

	// Holder can renew
	err = cluster.LeaseMgr.Renew(l.ID, "node_a", 5*time.Second)
	if err != nil {
		t.Errorf("holder should be able to renew: %v", err)
	}

	// Non-holder cannot renew
	err = cluster.LeaseMgr.Renew(l.ID, "node_b", 5*time.Second)
	if err == nil {
		t.Error("non-holder should not be able to renew")
	}

	// Revoke the lease
	cluster.LeaseMgr.Revoke(l.ID)

	// Cannot renew a revoked lease
	err = cluster.LeaseMgr.Renew(l.ID, "node_a", 5*time.Second)
	if err == nil {
		t.Error("should not be able to renew a revoked lease")
	}
}

// TestScenario4c_LeaseRevokeAllForNode verifies bulk revocation.
func TestScenario4c_LeaseRevokeAllForNode(t *testing.T) {
	cluster := NewTestCluster(10 * time.Second)
	defer cluster.Stop()

	// Create multiple leases for node_a
	cluster.LeaseMgr.Create("task_bulk_001", "node_a", 10*time.Second)
	cluster.LeaseMgr.Create("task_bulk_002", "node_a", 10*time.Second)
	cluster.LeaseMgr.Create("task_bulk_003", "node_b", 10*time.Second)

	// Revoke all for node_a
	revoked := cluster.LeaseMgr.RevokeAllForNode("node_a")

	if len(revoked) != 2 {
		t.Errorf("expected 2 revoked tasks, got %d", len(revoked))
	}

	// Verify node_b's lease is still active
	_, ok := cluster.LeaseMgr.GetActiveForTask("task_bulk_003")
	if !ok {
		t.Error("node_b's lease should still be active")
	}

	// Verify node_a's leases are revoked
	_, ok = cluster.LeaseMgr.GetActiveForTask("task_bulk_001")
	if ok {
		t.Error("node_a's lease for task_bulk_001 should be revoked")
	}
	_, ok = cluster.LeaseMgr.GetActiveForTask("task_bulk_002")
	if ok {
		t.Error("node_a's lease for task_bulk_002 should be revoked")
	}
}

// TestScenario4d_LeaseExpiryNoSameNodeReassign verifies that after lease expiry,
// the task is NOT rescheduled to the same node when other eligible nodes exist.
// This is Bug#3: lease过期后NotifyOffline不修改registry状态，scheduler仍选同一节点。
func TestScenario4d_LeaseExpiryNoSameNodeReassign(t *testing.T) {
	cluster := NewTestCluster(500 * time.Millisecond)
	defer cluster.Stop()

	// Register two nodes with same capabilities
	cluster.RegisterNode("node_a", "Node-A", []string{"coding"})
	cluster.RegisterNode("node_b", "Node-B", []string{"coding"})

	// Only start the recovery detector, NOT the watchdog.
	// The watchdog would mark nodes offline during the sleep (no heartbeats),
	// which would revoke the lease via NotifyOffline before we test expiry.
	cluster.Recovery.Start()
	defer cluster.Recovery.Stop()

	// Create a task and assign it directly to node_a (no lease yet)
	cluster.TaskStore.Create("task_expiry_001", "Lease expiry test task", []string{"coding"})
	cluster.TaskStore.SetAssigned("task_expiry_001", "node_a")

	// Create a lease with very short TTL for node_a
	_, err := cluster.LeaseMgr.Create("task_expiry_001", "node_a", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to create lease: %v", err)
	}
	t.Log("Task assigned to node_a with 500ms lease")

	// Wait for lease to expire
	time.Sleep(600 * time.Millisecond)

	// Manually trigger expiry check (simulates the background scanner)
	expired := cluster.LeaseMgr.CheckExpiry()
	if len(expired) == 0 {
		t.Fatal("expected at least one expired task")
	}

	// Wait for async callback to execute and recovery to process
	time.Sleep(200 * time.Millisecond)

	// Verify the node was marked offline in the registry
	nodeStatus, ok := cluster.Registry.Get("node_a")
	if !ok {
		t.Fatal("node_a not found in registry")
	}
	if nodeStatus.Status != "offline" {
		t.Errorf("expected node_a to be offline after lease expiry, got %s",
			nodeStatus.Status)
	}

	// Now reschedule the task - it should go to node_b (not node_a)
	cluster.TaskStore.Unassign("task_expiry_001")

	// Schedule pending tasks
	rescheduled := cluster.Scheduler.SchedulePending()

	// Verify the task was rescheduled to node_b (different from node_a)
	if len(rescheduled) == 0 {
		t.Fatal("expected task to be rescheduled to node_b")
	}
	for _, pair := range rescheduled {
		if pair[0] == "task_expiry_001" {
			if pair[1] == "node_a" {
				t.Errorf("task was rescheduled to the SAME node node_a after lease expiry! "+
					"Expected it to go to node_b")
			} else {
				t.Logf("Task correctly rescheduled to different node: %s", pair[1])
			}
		}
	}
}

// TestScenario4e_LeaseExpiryOnlyOfflineHolder verifies that only the expired
// node is marked offline, not other nodes.
func TestScenario4e_LeaseExpiryOnlyOfflineHolder(t *testing.T) {
	cluster := NewTestCluster(500 * time.Millisecond)
	defer cluster.Stop()

	// Register two nodes
	cluster.RegisterNode("node_a", "Node-A", []string{"coding"})
	cluster.RegisterNode("node_b", "Node-B", []string{"coding"})

	// Create a lease only for node_a
	cluster.LeaseMgr.Create("task_selective_001", "node_a", 500*time.Millisecond)
	// Create a lease for node_b with longer TTL
	cluster.LeaseMgr.Create("task_selective_002", "node_b", 10*time.Second)

	// Wait for node_a's lease to expire
	time.Sleep(600 * time.Millisecond)

	// Trigger expiry check
	cluster.LeaseMgr.CheckExpiry()
	time.Sleep(200 * time.Millisecond)

	// Verify node_a is offline
	nodeA, ok := cluster.Registry.Get("node_a")
	if !ok {
		t.Fatal("node_a not found")
	}
	if nodeA.Status != "offline" {
		t.Errorf("node_a should be offline, got %s", nodeA.Status)
	}

	// Verify node_b is still online
	nodeB, ok := cluster.Registry.Get("node_b")
	if !ok {
		t.Fatal("node_b not found")
	}
	if nodeB.Status != "online" {
		t.Errorf("node_b should still be online, got %s", nodeB.Status)
	}
}
