package integration

import (
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/sync"
)

// TestScenario3_StateSync verifies state synchronization between leader and followers:
// schedule task -> verify sync messages -> verify follower state store
func TestScenario3_StateSync(t *testing.T) {
	// Create two state stores: one for leader, one for follower
	leaderStore := sync.NewStateStore()
	followerStore := sync.NewStateStore()

	receiver := sync.NewFollowerReceiver(followerStore)

	// Step 1: Create a sync message from the leader
	taskSync := sync.TaskSync{
		TaskID:     "task_sync_001",
		Title:      "Sync test task",
		Status:     "assigned",
		AssignedTo: "node_a",
		Version:    1,
	}

	msg := sync.SyncMessage{
		Version:    1,
		SenderNode: "leader",
		TaskState:  &taskSync,
		EventType:  sync.EventTaskAssigned,
		Timestamp:  time.Now().UnixMilli(),
	}

	// Step 2: Apply the message to the follower's state store
	applied := leaderStore.Apply(msg)
	if !applied {
		t.Error("expected leader store to accept message")
	}

	// Step 3: Verify the follower received it
	followerApplied := receiver.HandleSyncMessage(msg)
	if !followerApplied {
		t.Error("expected follower to accept sync message")
	}

	// Step 4: Verify state is consistent between leader and follower
	leaderState, ok := leaderStore.Get("task_sync_001")
	if !ok {
		t.Fatal("task not found in leader store")
	}
	followerState, ok := followerStore.Get("task_sync_001")
	if !ok {
		t.Fatal("task not found in follower store")
	}

	if leaderState.Version != followerState.Version {
		t.Errorf("version mismatch: leader=%d, follower=%d", leaderState.Version, followerState.Version)
	}
	if leaderState.Status != followerState.Status {
		t.Errorf("status mismatch: leader=%s, follower=%s", leaderState.Status, followerState.Status)
	}
	if leaderState.AssignedTo != followerState.AssignedTo {
		t.Errorf("assigned_to mismatch: leader=%s, follower=%s", leaderState.AssignedTo, followerState.AssignedTo)
	}

	// Step 5: Send an updated sync message (version 2)
	taskSync2 := sync.TaskSync{
		TaskID:     "task_sync_001",
		Title:      "Sync test task",
		Status:     "completed",
		AssignedTo: "node_a",
		Version:    2,
	}
	msg2 := sync.SyncMessage{
		Version:    2,
		SenderNode: "leader",
		TaskState:  &taskSync2,
		EventType:  sync.EventTaskCompleted,
		Timestamp:  time.Now().UnixMilli(),
	}

	leaderStore.Apply(msg2)
	receiver.HandleSyncMessage(msg2)

	// Step 6: Verify version incremented
	leaderState2, _ := leaderStore.Get("task_sync_001")
	followerState2, _ := followerStore.Get("task_sync_001")
	if leaderState2.Version != 2 {
		t.Errorf("expected leader version 2, got %d", leaderState2.Version)
	}
	if followerState2.Version != 2 {
		t.Errorf("expected follower version 2, got %d", followerState2.Version)
	}
	if followerState2.Status != "completed" {
		t.Errorf("expected follower status 'completed', got '%s'", followerState2.Status)
	}

	// Step 7: Verify stale message (version 1) is rejected
	staleMsg := sync.SyncMessage{
		Version:    1,
		SenderNode: "leader",
		TaskState: &sync.TaskSync{
			TaskID:  "task_sync_001",
			Version: 1,
			Status:  "ready",
		},
		EventType: sync.EventTaskCreated,
		Timestamp: time.Now().UnixMilli(),
	}

	staleApplied := leaderStore.Apply(staleMsg)
	if staleApplied {
		t.Error("expected stale message to be rejected by leader store")
	}

	followerStaleApplied := receiver.HandleSyncMessage(staleMsg)
	if followerStaleApplied {
		t.Error("expected stale message to be rejected by follower store")
	}

	// Step 8: Verify global version tracking
	if leaderStore.Version() != 2 {
		t.Errorf("expected leader global version 2, got %d", leaderStore.Version())
	}
	if followerStore.Version() != 2 {
		t.Errorf("expected follower global version 2, got %d", followerStore.Version())
	}
}
