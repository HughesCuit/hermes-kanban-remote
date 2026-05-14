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

// TestSyncReceive_EndToEnd verifies the full sync flow:
// 1) Leader registers a follower via join API (with endpoint)
// 2) Leader pushes sync state to follower via POST /api/v1/sync/receive
// 3) Follower's state store is updated
func TestSyncReceive_EndToEnd(t *testing.T) {
	// --- Set up follower node (HTTP server) ---
	// Follower needs its own registry, scheduler, etc. for the API server
	followerRegistry := cluster.NewRegistry()
	followerSched := scheduler.NewScheduler(followerRegistry, scheduler.NewTaskStore(), lease.NewManager(), 30*time.Second)
	followerLeaseMgr := lease.NewManager()
	followerRecLog := recovery.NewLog()
	followerStateStore := sync.NewStateStore() // separate state store for the API server
	followerSyncReceiver := sync.NewFollowerReceiver(followerStateStore)

	// Follower doesn't need leaderSync (it only receives, doesn't push)
	followerResolver := workflow.NewResolver(scheduler.NewTaskStore())
	followerServer := api.NewServer(
		followerRegistry,
		followerSched,
		followerLeaseMgr,
		recovery.NewDetector(nil, nil, followerLeaseMgr, followerRecLog),
		followerRecLog,
		followerStateStore,
		followerSyncReceiver,
		nil, // no leaderSync on follower
		followerResolver,
	)

	followerHTTP := httptest.NewServer(followerServer.Router)
	defer followerHTTP.Close()

	// --- Set up leader node (HTTP server) ---
	leaderStore := sync.NewStateStore()
	leaderRegistry := cluster.NewRegistry()
	leaderSched := scheduler.NewScheduler(leaderRegistry, scheduler.NewTaskStore(), lease.NewManager(), 30*time.Second)
	leaderLeaseMgr := lease.NewManager()
	leaderRecLog := recovery.NewLog()
	leaderReceiver := sync.NewFollowerReceiver(leaderStore)
	leaderPusher := sync.NewHTTPPusher()
	leaderSync := sync.NewLeaderSync(leaderStore, leaderPusher)

	leaderResolver := workflow.NewResolver(scheduler.NewTaskStore())
	leaderServer := api.NewServer(
		leaderRegistry,
		leaderSched,
		leaderLeaseMgr,
		recovery.NewDetector(nil, nil, leaderLeaseMgr, leaderRecLog),
		leaderRecLog,
		leaderStore,
		leaderReceiver,
		leaderSync,
		leaderResolver,
	)

	leaderHTTP := httptest.NewServer(leaderServer.Router)
	defer leaderHTTP.Close()

	// --- Step 1: Follower joins leader with its endpoint ---
	joinBody, _ := json.Marshal(map[string]interface{}{
		"node_name":     "follower-1",
		"capabilities":  []string{"compute"},
		"endpoint":      followerHTTP.URL, // follower's base URL
	})

	resp, err := http.Post(leaderHTTP.URL+"/api/v1/nodes/join", "application/json", bytes.NewReader(joinBody))
	if err != nil {
		t.Fatalf("join request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var joinResp map[string]string
	json.NewDecoder(resp.Body).Decode(&joinResp)
	if joinResp["status"] != "registered" {
		t.Fatalf("expected status=registered, got %s", joinResp["status"])
	}

	// --- Step 2: Leader pushes sync state to follower ---
	// Create a sync message
	msg := sync.SyncMessage{
		Version:    1,
		SenderNode: "leader",
		TaskState: &sync.TaskSync{
			TaskID:     "task_e2e_001",
			Title:      "E2E sync test task",
			Status:     "assigned",
			AssignedTo: "follower-1",
			Version:    1,
		},
		EventType:  sync.EventTaskAssigned,
		Timestamp:  time.Now().UnixMilli(),
	}

	// Leader POSTs to follower's /api/v1/sync/receive
	msgBody, _ := json.Marshal(msg)
	syncResp, err := http.Post(
		followerHTTP.URL+"/api/v1/sync/receive",
		"application/json",
		bytes.NewReader(msgBody),
	)
	if err != nil {
		t.Fatalf("sync receive request failed: %v", err)
	}
	defer syncResp.Body.Close()

	if syncResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from sync/receive, got %d", syncResp.StatusCode)
	}

	var syncResult map[string]bool
	json.NewDecoder(syncResp.Body).Decode(&syncResult)
	if !syncResult["applied"] {
		t.Error("expected applied=true from follower sync/receive")
	}

	// --- Step 3: Verify follower state store was updated ---
	state, ok := followerStateStore.Get("task_e2e_001")
	if !ok {
		t.Fatal("task not found in follower state store after sync")
	}

	if state.Status != "assigned" {
		t.Errorf("expected status 'assigned', got '%s'", state.Status)
	}
	if state.AssignedTo != "follower-1" {
		t.Errorf("expected assigned_to 'follower-1', got '%s'", state.AssignedTo)
	}
	if state.Version != 1 {
		t.Errorf("expected version 1, got %d", state.Version)
	}

	// --- Step 4: Verify follower's sync status endpoint ---
	statusResp, err := http.Get(followerHTTP.URL + "/api/v1/sync/status")
	if err != nil {
		t.Fatalf("sync status request failed: %v", err)
	}
	defer statusResp.Body.Close()

	var statusResult map[string]int64
	json.NewDecoder(statusResp.Body).Decode(&statusResult)
	if statusResult["version"] != 1 {
		t.Errorf("expected global version 1, got %d", statusResult["version"])
	}

	t.Log("end-to-end sync test passed: leader -> follower sync via HTTP")
}

// TestSyncReceive_StaleMessageRejected verifies that stale sync messages
// (lower version) are rejected by the follower's state store.
func TestSyncReceive_StaleMessageRejected(t *testing.T) {
	followerStore := sync.NewStateStore()
	followerReceiver := sync.NewFollowerReceiver(followerStore)

	// Apply a version 2 message first
	msgV2 := sync.SyncMessage{
		Version:    2,
		SenderNode: "leader",
		TaskState: &sync.TaskSync{
			TaskID:  "task_stale_001",
			Title:   "Stale test",
			Status:  "completed",
			Version: 2,
		},
		EventType: sync.EventTaskCompleted,
		Timestamp: time.Now().UnixMilli(),
	}

	if !followerReceiver.HandleSyncMessage(msgV2) {
		t.Fatal("expected v2 message to be applied")
	}

	// Now try to apply a stale version 1 message
	msgV1 := sync.SyncMessage{
		Version:    1,
		SenderNode: "leader",
		TaskState: &sync.TaskSync{
			TaskID:  "task_stale_001",
			Title:   "Stale test",
			Status:  "assigned",
			Version: 1,
		},
		EventType: sync.EventTaskAssigned,
		Timestamp: time.Now().UnixMilli(),
	}

	if followerReceiver.HandleSyncMessage(msgV1) {
		t.Error("expected stale v1 message to be rejected")
	}

	// Verify state is still v2
	state, ok := followerStore.Get("task_stale_001")
	if !ok {
		t.Fatal("task not found in store")
	}
	if state.Status != "completed" {
		t.Errorf("expected status 'completed' (v2), got '%s'", state.Status)
	}

	t.Log("stale message rejection test passed")
}

// TestLeaderSync_AddFollower_Multiple verifies adding multiple followers.
func TestLeaderSync_AddFollower_Multiple(t *testing.T) {
	store := sync.NewStateStore()
	pusher := sync.NewHTTPPusher()
	ls := sync.NewLeaderSync(store, pusher)

	// Add multiple followers
	ls.AddFollower("http://follower1:8080")
	ls.AddFollower("http://follower2:8080")
	ls.AddFollower("http://follower3:8080")

	// Verify by pushing - should attempt to push to all 3
	// (we can't easily verify the push targets without mocking,
	// but we can verify no panic occurs)
	msg := sync.SyncMessage{
		Version:    1,
		SenderNode: "leader",
		TaskState: &sync.TaskSync{
			TaskID:  "task_multi_001",
			Title:   "Multi follower test",
			Status:  "ready",
			Version: 1,
		},
		EventType: sync.EventTaskCreated,
		Timestamp: time.Now().UnixMilli(),
	}

	// This should not panic even though followers are unreachable
	ls.PushTaskState(*msg.TaskState, msg.EventType, msg.SenderNode)

	// Give goroutines a moment to attempt pushes
	time.Sleep(100 * time.Millisecond)

	// Remove a follower and verify
	ls.RemoveFollower("http://follower2:8080")

	// Push again - should only attempt 2 pushes
	ls.PushTaskState(*msg.TaskState, msg.EventType, msg.SenderNode)
	time.Sleep(100 * time.Millisecond)

	t.Log("multi-follower add/remove test passed")
}

// TestSyncReceive_VersionIncrement verifies version increments across multiple sync messages.
func TestSyncReceive_VersionIncrement(t *testing.T) {
	followerStore := sync.NewStateStore()
	followerReceiver := sync.NewFollowerReceiver(followerStore)

	for i := int64(1); i <= 5; i++ {
		msg := sync.SyncMessage{
			Version:    i,
			SenderNode: "leader",
			TaskState: &sync.TaskSync{
				TaskID:  fmt.Sprintf("task_ver_%d", i),
				Title:   fmt.Sprintf("Task version %d", i),
				Status:  "assigned",
				Version: i,
			},
			EventType: sync.EventTaskAssigned,
			Timestamp: time.Now().UnixMilli(),
		}

		if !followerReceiver.HandleSyncMessage(msg) {
			t.Fatalf("expected v%d message to be applied", i)
		}
	}

	// Verify global version
	if followerStore.Version() != 5 {
		t.Errorf("expected global version 5, got %d", followerStore.Version())
	}

	// Verify each task exists with correct version
	for i := int64(1); i <= 5; i++ {
		state, ok := followerStore.Get(fmt.Sprintf("task_ver_%d", i))
		if !ok {
			t.Errorf("task_ver_%d not found in store", i)
			continue
		}
		if state.Version != i {
			t.Errorf("task_ver_%d: expected version %d, got %d", i, i, state.Version)
		}
	}

	t.Log("version increment test passed")
}
