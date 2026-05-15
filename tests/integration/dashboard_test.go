package integration

import (
	"encoding/json"
	"io"
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
	"github.com/heventure/hermes-agent-cluster/internal/visualization"
	"github.com/heventure/hermes-agent-cluster/internal/workflow"
)

// setupDashboardServer creates a fully wired test server with ClusterView.
func setupDashboardServer(t *testing.T) *httptest.Server {
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
	resolver := workflow.NewResolver(taskStore)
	clusterView := visualization.NewClusterView(registry, taskStore, leaseMgr, recLog, resolver)

	srv := api.NewServer(registry, sched, leaseMgr, detector, recLog, stateStore, receiver, leaderSync, resolver,
		api.WithClusterView(clusterView))
	ts := httptest.NewServer(srv.Router)
	return ts
}

// --- Dashboard HTML serving tests ---

func TestDashboard_IndexPage(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/dashboard/")
	if err != nil {
		t.Fatalf("GET /dashboard/ failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}
	html := string(body)

	// Verify HTML structure
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("response is not valid HTML (missing DOCTYPE)")
	}
	if !strings.Contains(html, "<title>Hermes Agent Cluster") {
		t.Error("missing dashboard title")
	}
	if !strings.Contains(html, "Hermes Agent Cluster") {
		t.Error("missing dashboard branding")
	}

	// Verify dark theme CSS
	if !strings.Contains(html, "--bg-primary") {
		t.Error("missing CSS custom properties (dark theme)")
	}
	if !strings.Contains(html, "#0d1117") {
		t.Error("missing dark background color")
	}

	// Verify dashboard sections exist
	sections := []string{
		"summary-grid",
		"node-grid",
		"task-tbody",
		"graph-container",
		"recovery-stats",
	}
	for _, s := range sections {
		if !strings.Contains(html, s) {
			t.Errorf("missing dashboard section: %s", s)
		}
	}

	// Verify filter controls
	if !strings.Contains(html, "filter-status") {
		t.Error("missing status filter")
	}
	if !strings.Contains(html, "filter-node") {
		t.Error("missing node filter")
	}

	// Verify API endpoint references in JavaScript
	// The JS constructs URLs as API + path, so check for the path segments
	apiRefs := []string{
		"/api/v1",   // base API prefix
		"/status",   // status endpoint
		"/nodes",    // nodes endpoint
		"/workflow/graph", // graph endpoint
		"/recovery/stats", // recovery endpoint
		"/sync/status",    // sync endpoint
	}
	for _, ref := range apiRefs {
		if !strings.Contains(html, ref) {
			t.Errorf("dashboard JS missing API reference: %s", ref)
		}
	}

	// Verify auto-refresh
	if !strings.Contains(html, "setInterval") {
		t.Error("missing auto-refresh (setInterval)")
	}
	if !strings.Contains(html, "5000") {
		t.Error("missing 5s refresh interval")
	}

	// Verify JavaScript functions exist
	jsFuncs := []string{
		"function fetchJSON",
		"function renderSummary",
		"function renderNodes",
		"function renderTasks",
		"function renderGraph",
		"function renderRecovery",
		"function refresh",
	}
	for _, f := range jsFuncs {
		if !strings.Contains(html, f) {
			t.Errorf("missing JavaScript function: %s", f)
		}
	}
}

func TestDashboard_RedirectBarePath(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	// /dashboard should redirect to /dashboard/
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(ts.URL + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("expected 301 redirect, got %d", resp.StatusCode)
	}
	location := resp.Header.Get("Location")
	if location != "/dashboard/" {
		t.Errorf("expected redirect to /dashboard/, got %q", location)
	}
}

func TestDashboard_CSSFile(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	// The dashboard only serves index.html (no separate CSS files),
	// but verify the handler returns 404 for missing files
	resp, err := http.Get(ts.URL + "/dashboard/nonexistent.js")
	if err != nil {
		t.Fatalf("GET /dashboard/nonexistent.js failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for missing file, got %d", resp.StatusCode)
	}
}

// --- API endpoint tests that dashboard JS calls ---

func TestDashboard_APIStatus_Empty(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/status")
	if err != nil {
		t.Fatalf("GET /api/v1/status failed: %v", err)
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
	if result.Summary.TotalNodes != 0 {
		t.Errorf("expected 0 nodes, got %d", result.Summary.TotalNodes)
	}
	if result.Summary.TotalTasks != 0 {
		t.Errorf("expected 0 tasks, got %d", result.Summary.TotalTasks)
	}
}

func TestDashboard_APIStatus_WithData(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	// Register a node
	nodeResp, err := http.Post(ts.URL+"/api/v1/nodes/join", "application/json",
		strings.NewReader(`{"node_name":"worker-1","capabilities":["gpu","coding"]}`))
	if err != nil {
		t.Fatalf("join node failed: %v", err)
	}
	nodeResp.Body.Close()

	// Submit 3 tasks
	for i := 0; i < 3; i++ {
		taskResp, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
			strings.NewReader(`{"title":"dashboard task","requires":[]}`))
		if err != nil {
			t.Fatalf("submit task %d failed: %v", i, err)
		}
		taskResp.Body.Close()
	}

	// Get status
	resp, err := http.Get(ts.URL + "/api/v1/status")
	if err != nil {
		t.Fatalf("GET /api/v1/status failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Entries []struct {
			TaskID     string   `json:"task_id"`
			TaskTitle  string   `json:"task_title"`
			TaskStatus string   `json:"task_status"`
			NodeID     string   `json:"node_id"`
			NodeName   string   `json:"node_name"`
			Caps       []string `json:"capabilities"`
		} `json:"entries"`
		Summary struct {
			TotalNodes    int            `json:"total_nodes"`
			OnlineNodes   int            `json:"online_nodes"`
			TotalTasks    int            `json:"total_tasks"`
			ActiveLeases  int            `json:"active_leases"`
			TasksByStatus map[string]int `json:"tasks_by_status"`
		} `json:"summary"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify summary
	if result.Summary.TotalNodes != 1 {
		t.Errorf("expected 1 node, got %d", result.Summary.TotalNodes)
	}
	if result.Summary.TotalTasks != 3 {
		t.Errorf("expected 3 tasks, got %d", result.Summary.TotalTasks)
	}

	// Verify entries
	if len(result.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result.Entries))
	}

	// All tasks should be ready (no requirements, no nodes matching)
	for _, e := range result.Entries {
		if e.TaskTitle != "dashboard task" {
			t.Errorf("unexpected title: %s", e.TaskTitle)
		}
	}
}

func TestDashboard_APINodes(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	// Register 2 nodes
	for _, name := range []string{"gpu-node", "cpu-node"} {
		resp, err := http.Post(ts.URL+"/api/v1/nodes/join", "application/json",
			strings.NewReader(`{"node_name":"`+name+`","capabilities":["`+name[:3]+`"]}`))
		if err != nil {
			t.Fatalf("join node %s failed: %v", name, err)
		}
		resp.Body.Close()
	}

	// Get nodes
	resp, err := http.Get(ts.URL + "/api/v1/nodes")
	if err != nil {
		t.Fatalf("GET /api/v1/nodes failed: %v", err)
	}
	defer resp.Body.Close()

	var nodes []struct {
		ID           string   `json:"id"`
		Name         string   `json:"name"`
		Status       string   `json:"status"`
		Capabilities []string `json:"capabilities"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	// Verify node details
	names := map[string]bool{}
	for _, n := range nodes {
		names[n.Name] = true
		if n.Status == "" {
			t.Errorf("node %s has empty status", n.Name)
		}
		if len(n.Capabilities) == 0 {
			t.Errorf("node %s has no capabilities", n.Name)
		}
	}
	if !names["gpu-node"] || !names["cpu-node"] {
		t.Errorf("expected gpu-node and cpu-node, got %v", names)
	}
}

func TestDashboard_APIWorkflowGraph_Empty(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/workflow/graph")
	if err != nil {
		t.Fatalf("GET /api/v1/workflow/graph failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var graph struct {
		Nodes []interface{} `json:"nodes"`
		Edges []interface{} `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&graph); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(graph.Nodes) != 0 {
		t.Errorf("expected 0 nodes for empty graph, got %d", len(graph.Nodes))
	}
}

func TestDashboard_APIWorkflowGraph_WithDeps(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	// Submit task A
	respA, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"task A","requires":[]}`))
	if err != nil {
		t.Fatalf("submit task A failed: %v", err)
	}
	var taskA struct{ ID string }
	json.NewDecoder(respA.Body).Decode(&taskA)
	respA.Body.Close()

	// Submit task B
	respB, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"task B","requires":[]}`))
	if err != nil {
		t.Fatalf("submit task B failed: %v", err)
	}
	var taskB struct{ ID string }
	json.NewDecoder(respB.Body).Decode(&taskB)
	respB.Body.Close()

	// Set dependency: B depends on A
	depResp, err := http.Post(ts.URL+"/api/v1/tasks/"+taskB.ID+"/dependencies", "application/json",
		strings.NewReader(`{"depends_on":["`+taskA.ID+`"]}`))
	if err != nil {
		t.Fatalf("set dependencies failed: %v", err)
	}
	depResp.Body.Close()

	// Get graph
	resp, err := http.Get(ts.URL + "/api/v1/workflow/graph")
	if err != nil {
		t.Fatalf("GET /api/v1/workflow/graph failed: %v", err)
	}
	defer resp.Body.Close()

	var graph struct {
		Nodes []struct {
			ID     string `json:"id"`
			Title  string `json:"title"`
			Status string `json:"status"`
		} `json:"nodes"`
		Edges []struct {
			From string `json:"from"`
			To   string `json:"to"`
		} `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&graph); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(graph.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(graph.Edges))
	}
	if graph.Edges[0].From != taskA.ID || graph.Edges[0].To != taskB.ID {
		t.Errorf("edge mismatch: expected %s -> %s, got %s -> %s",
			taskA.ID, taskB.ID, graph.Edges[0].From, graph.Edges[0].To)
	}
}

func TestDashboard_APIRecoveryStats(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/recovery/stats")
	if err != nil {
		t.Fatalf("GET /api/v1/recovery/stats failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var stats struct {
		TotalEvents      int `json:"total_events"`
		LeasesRevoked    int `json:"leases_revoked"`
		TasksRescheduled int `json:"tasks_rescheduled"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	// Empty cluster should have 0 stats
	if stats.TotalEvents != 0 {
		t.Errorf("expected 0 total events, got %d", stats.TotalEvents)
	}
}

func TestDashboard_APISyncStatus(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/sync/status")
	if err != nil {
		t.Fatalf("GET /api/v1/sync/status failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var status struct {
		Version int64 `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	// Empty cluster should start at version 0
	if status.Version != 0 {
		t.Errorf("expected version 0, got %d", status.Version)
	}
}

// --- End-to-end dashboard scenario test ---

func TestDashboard_E2E_Scenario(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	// Step 1: Register nodes
	for _, tc := range []struct {
		name string
		caps []string
	}{
		{"gpu-worker", []string{"gpu", "cuda", "python"}},
		{"cpu-worker", []string{"cpu", "bash", "docker"}},
	} {
		resp, err := http.Post(ts.URL+"/api/v1/nodes/join", "application/json",
			strings.NewReader(`{"node_name":"`+tc.name+`","capabilities":`+jsonStrSlice(tc.caps)+`}`))
		if err != nil {
			t.Fatalf("join node %s failed: %v", tc.name, err)
		}
		resp.Body.Close()
	}

	// Step 2: Submit tasks with dependencies
	// Task A: no deps
	respA, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"data ingestion","requires":["cpu"]}`))
	if err != nil {
		t.Fatalf("submit task A failed: %v", err)
	}
	var taskA struct{ ID string }
	json.NewDecoder(respA.Body).Decode(&taskA)
	respA.Body.Close()

	// Task B: depends on A
	respB, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"model training","requires":["gpu"]}`))
	if err != nil {
		t.Fatalf("submit task B failed: %v", err)
	}
	var taskB struct{ ID string }
	json.NewDecoder(respB.Body).Decode(&taskB)
	respB.Body.Close()

	// Set dependency: B depends on A
	depResp, err := http.Post(ts.URL+"/api/v1/tasks/"+taskB.ID+"/dependencies", "application/json",
		strings.NewReader(`{"depends_on":["`+taskA.ID+`"]}`))
	if err != nil {
		t.Fatalf("set dependencies failed: %v", err)
	}
	depResp.Body.Close()

	// Step 3: Verify all dashboard API endpoints return valid data
	t.Run("status", func(t *testing.T) {
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
				TotalNodes  int `json:"total_nodes"`
				TotalTasks  int `json:"total_tasks"`
				OnlineNodes int `json:"online_nodes"`
			} `json:"summary"`
		}
		json.NewDecoder(resp.Body).Decode(&result)
		if result.Summary.TotalNodes != 2 {
			t.Errorf("expected 2 nodes, got %d", result.Summary.TotalNodes)
		}
		if result.Summary.TotalTasks != 2 {
			t.Errorf("expected 2 tasks, got %d", result.Summary.TotalTasks)
		}
	})

	t.Run("nodes", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/v1/nodes")
		if err != nil {
			t.Fatalf("GET /nodes failed: %v", err)
		}
		defer resp.Body.Close()
		var nodes []struct {
			Name         string `json:"name"`
			Capabilities []string `json:"capabilities"`
		}
		json.NewDecoder(resp.Body).Decode(&nodes)
		if len(nodes) != 2 {
			t.Fatalf("expected 2 nodes, got %d", len(nodes))
		}
	})

	t.Run("graph", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/v1/workflow/graph")
		if err != nil {
			t.Fatalf("GET /graph failed: %v", err)
		}
		defer resp.Body.Close()
		var graph struct {
			Nodes []interface{} `json:"nodes"`
			Edges []interface{} `json:"edges"`
		}
		json.NewDecoder(resp.Body).Decode(&graph)
		if len(graph.Nodes) != 2 {
			t.Errorf("expected 2 graph nodes, got %d", len(graph.Nodes))
		}
		if len(graph.Edges) != 1 {
			t.Errorf("expected 1 graph edge, got %d", len(graph.Edges))
		}
	})

	t.Run("recovery", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/v1/recovery/stats")
		if err != nil {
			t.Fatalf("GET /recovery/stats failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("sync", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/v1/sync/status")
		if err != nil {
			t.Fatalf("GET /sync/status failed: %v", err)
		}
		defer resp.Body.Close()
		var status struct {
			Version int64 `json:"version"`
		}
		json.NewDecoder(resp.Body).Decode(&status)
		if status.Version < 0 {
			t.Errorf("expected non-negative version, got %d", status.Version)
		}
	})

	t.Run("dashboard_html", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/dashboard/")
		if err != nil {
			t.Fatalf("GET /dashboard/ failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "Hermes Agent Cluster") {
			t.Error("dashboard HTML missing title")
		}
	})

	// Step 4: Complete task A, verify status updates
	t.Run("complete_task_a", func(t *testing.T) {
		resp, err := http.Post(ts.URL+"/api/v1/tasks/"+taskA.ID+"/complete", "application/json", nil)
		if err != nil {
			t.Fatalf("complete task A failed: %v", err)
		}
		resp.Body.Close()

		// Verify status now shows completed
		statusResp, err := http.Get(ts.URL + "/api/v1/status")
		if err != nil {
			t.Fatalf("GET /status failed: %v", err)
		}
		defer statusResp.Body.Close()

		var result struct {
			Summary struct {
				TasksByStatus map[string]int `json:"tasks_by_status"`
			} `json:"summary"`
		}
		json.NewDecoder(statusResp.Body).Decode(&result)
		if result.Summary.TasksByStatus["completed"] != 1 {
			t.Errorf("expected 1 completed task, got %d", result.Summary.TasksByStatus["completed"])
		}
	})
}

// --- Dashboard without ClusterView (graceful degradation) ---

func TestDashboard_APIStatus_NoClusterView(t *testing.T) {
	// Setup server WITHOUT ClusterView (like the basic status tests)
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
	resolver := workflow.NewResolver(taskStore)

	srv := api.NewServer(registry, sched, leaseMgr, detector, recLog, stateStore, receiver, leaderSync, resolver)
	ts := httptest.NewServer(srv.Router)
	defer ts.Close()

	// Dashboard should still be served
	resp, err := http.Get(ts.URL + "/dashboard/")
	if err != nil {
		t.Fatalf("GET /dashboard/ failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Cluster viz endpoints should return 503
	for _, ep := range []string{"/api/v1/cluster/topology", "/api/v1/cluster/metrics", "/api/v1/cluster/timeline"} {
		resp, err := http.Get(ts.URL + ep)
		if err != nil {
			t.Fatalf("GET %s failed: %v", ep, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("expected 503 for %s without ClusterView, got %d", ep, resp.StatusCode)
		}
	}

	// Status should still work
	statusResp, err := http.Get(ts.URL + "/api/v1/status")
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	statusResp.Body.Close()
	if statusResp.StatusCode != 200 {
		t.Errorf("expected 200 for /status, got %d", statusResp.StatusCode)
	}
}

// --- Content-Type and security headers ---

func TestDashboard_ContentType(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/dashboard/")
	if err != nil {
		t.Fatalf("GET /dashboard/ failed: %v", err)
	}
	defer resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("expected text/html content type, got %s", ct)
	}
}

func TestDashboard_APICodeEndpoint(t *testing.T) {
	ts := setupDashboardServer(t)
	defer ts.Close()

	// Verify all dashboard-calling endpoints return valid JSON
	endpoints := []string{
		"/api/v1/status",
		"/api/v1/nodes",
		"/api/v1/workflow/graph",
		"/api/v1/recovery/stats",
		"/api/v1/sync/status",
	}
	for _, ep := range endpoints {
		resp, err := http.Get(ts.URL + ep)
		if err != nil {
			t.Fatalf("GET %s failed: %v", ep, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("expected 200 for %s, got %d", ep, resp.StatusCode)
		}
		ct := resp.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/json") {
			t.Errorf("expected JSON content type for %s, got %s", ep, ct)
		}
	}
}

// --- Helper ---

func jsonStrSlice(ss []string) string {
	b, _ := json.Marshal(ss)
	return string(b)
}
