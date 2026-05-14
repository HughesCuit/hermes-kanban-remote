package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/api"
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/recovery"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
	"github.com/heventure/hermes-agent-cluster/internal/sync"
)

func setupStatusServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := cluster.NewRegistry()
	taskStore := scheduler.NewTaskStore()
	leaseMgr := lease.NewManager()
	recLog := recovery.NewLog()
	stateStore := sync.NewStateStore()
	receiver := sync.NewFollowerReceiver(stateStore)
	pusher := sync.NewHTTPPusher()
	leaderSync := sync.NewLeaderSync(stateStore, pusher)
	sched := scheduler.NewScheduler(registry, taskStore, leaseMgr, 5*time.Minute)
	detector := recovery.NewDetector(nil, nil, leaseMgr, recLog)

	srv := api.NewServer(registry, sched, leaseMgr, detector, recLog, stateStore, receiver, leaderSync)
	ts := httptest.NewServer(srv.Router)
	return ts
}

func TestStatus_EmptyCluster(t *testing.T) {
	ts := setupStatusServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/status")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Entries []interface{} `json:"entries"`
		Summary struct {
			TotalNodes   int `json:"total_nodes"`
			OnlineNodes  int `json:"online_nodes"`
			TotalTasks   int `json:"total_tasks"`
			ActiveLeases int `json:"active_leases"`
		} `json:"summary"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if result.Summary.TotalTasks != 0 {
		t.Errorf("expected 0 tasks, got %d", result.Summary.TotalTasks)
	}
	if result.Summary.TotalNodes != 0 {
		t.Errorf("expected 0 nodes, got %d", result.Summary.TotalNodes)
	}
}

func TestStatus_WithTask(t *testing.T) {
	ts := setupStatusServer(t)
	defer ts.Close()

	// Submit a task with no requirements (will be "ready" status)
	submitResp, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"test task","requires":[]}`))
	if err != nil {
		t.Fatalf("submit task failed: %v", err)
	}
	submitResp.Body.Close()

	// Get status
	resp, err := http.Get(ts.URL + "/api/v1/status")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Entries []struct {
			TaskID     string `json:"task_id"`
			TaskTitle  string `json:"task_title"`
			TaskStatus string `json:"task_status"`
		} `json:"entries"`
		Summary struct {
			TotalTasks int `json:"total_tasks"`
		} `json:"summary"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if result.Summary.TotalTasks != 1 {
		t.Errorf("expected 1 task, got %d", result.Summary.TotalTasks)
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result.Entries))
	}
	if result.Entries[0].TaskTitle != "test task" {
		t.Errorf("expected title 'test task', got %q", result.Entries[0].TaskTitle)
	}
	if result.Entries[0].TaskStatus != "ready" {
		t.Errorf("expected status 'ready', got %q", result.Entries[0].TaskStatus)
	}
}

func TestStatus_FilterByStatus(t *testing.T) {
	ts := setupStatusServer(t)
	defer ts.Close()

	// Submit a task
	submitResp, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"filter task","requires":[]}`))
	if err != nil {
		t.Fatalf("submit task failed: %v", err)
	}
	submitResp.Body.Close()

	// Filter by "completed" — should return 0
	resp, err := http.Get(ts.URL + "/api/v1/status?status=completed")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Entries []interface{} `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries for status=completed, got %d", len(result.Entries))
	}

	// Filter by "ready" — should return 1
	resp, err = http.Get(ts.URL + "/api/v1/status?status=ready")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()

	var readyResult struct {
		Entries []interface{} `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&readyResult); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(readyResult.Entries) != 1 {
		t.Errorf("expected 1 entry for status=ready, got %d", len(readyResult.Entries))
	}
}

func TestStatus_SummaryCounts(t *testing.T) {
	ts := setupStatusServer(t)
	defer ts.Close()

	// Submit 3 tasks
	for i := 0; i < 3; i++ {
		submitResp, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
			strings.NewReader(`{"title":"task","requires":[]}`))
		if err != nil {
			t.Fatalf("submit task %d failed: %v", i, err)
		}
		submitResp.Body.Close()
	}

	resp, err := http.Get(ts.URL + "/api/v1/status")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Summary struct {
			TotalTasks    int            `json:"total_tasks"`
			TasksByStatus map[string]int `json:"tasks_by_status"`
		} `json:"summary"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if result.Summary.TotalTasks != 3 {
		t.Errorf("expected 3 total tasks, got %d", result.Summary.TotalTasks)
	}
	if result.Summary.TasksByStatus["ready"] != 3 {
		t.Errorf("expected 3 ready tasks, got %d", result.Summary.TasksByStatus["ready"])
	}
}

func TestStatus_FilterByNode(t *testing.T) {
	ts := setupStatusServer(t)
	defer ts.Close()

	// Submit a task (will be unassigned)
	submitResp, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"node task","requires":[]}`))
	if err != nil {
		t.Fatalf("submit task failed: %v", err)
	}
	submitResp.Body.Close()

	// Filter by non-existent node — should return 0
	resp, err := http.Get(ts.URL + "/api/v1/status?node=node_nonexistent")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Entries []interface{} `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries for nonexistent node, got %d", len(result.Entries))
	}
}
