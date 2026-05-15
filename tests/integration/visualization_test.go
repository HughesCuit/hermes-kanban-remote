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
	"github.com/heventure/hermes-agent-cluster/internal/visualization"
	"github.com/heventure/hermes-agent-cluster/internal/workflow"
)

func setupVizServer(t *testing.T) *httptest.Server {
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

	// Register self node (mirrors main.go behavior)
	registry.Register(&cluster.Node{
		ID:           "node_main",
		Name:         "main",
		Capabilities: []string{"coding", "scheduler"},
	})

	srv := api.NewServer(registry, sched, leaseMgr, detector, recLog, stateStore, receiver, leaderSync, resolver,
		api.WithClusterView(clusterView))
	ts := httptest.NewServer(srv.Router)
	return ts
}

func TestClusterViz_EmptyCluster(t *testing.T) {
	ts := setupVizServer(t)
	defer ts.Close()

	// Topology should have 1 node (self) but no tasks
	resp, err := http.Get(ts.URL + "/api/v1/cluster/topology")
	if err != nil {
		t.Fatalf("GET /cluster/topology failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var topo struct {
		Nodes []interface{} `json:"nodes"`
		Tasks []interface{} `json:"tasks"`
		Edges []interface{} `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&topo); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(topo.Nodes) != 1 {
		t.Errorf("expected 1 node (self), got %d", len(topo.Nodes))
	}
	if len(topo.Tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(topo.Tasks))
	}
}

func TestClusterViz_MetricsEmpty(t *testing.T) {
	ts := setupVizServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/cluster/metrics")
	if err != nil {
		t.Fatalf("GET /cluster/metrics failed: %v", err)
	}
	defer resp.Body.Close()

	var metrics struct {
		Nodes []interface{} `json:"nodes"`
		Tasks struct {
			Total          int            `json:"total"`
			ByStatus       map[string]int `json:"by_status"`
			CompletionRate float64        `json:"completion_rate"`
		} `json:"tasks"`
		Leases struct {
			ActiveCount int `json:"active_count"`
		} `json:"leases"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if metrics.Tasks.Total != 0 {
		t.Errorf("expected 0 total tasks, got %d", metrics.Tasks.Total)
	}
	// 1 node (self)
	if len(metrics.Nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(metrics.Nodes))
	}
	if metrics.Leases.ActiveCount != 0 {
		t.Errorf("expected 0 active leases, got %d", metrics.Leases.ActiveCount)
	}
}

func TestClusterViz_TimelineEmpty(t *testing.T) {
	ts := setupVizServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/cluster/timeline")
	if err != nil {
		t.Fatalf("GET /cluster/timeline failed: %v", err)
	}
	defer resp.Body.Close()

	var timeline struct {
		Events []interface{} `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&timeline); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if len(timeline.Events) != 0 {
		t.Errorf("expected 0 events, got %d", len(timeline.Events))
	}
}

func TestClusterViz_TopologyWithNodes(t *testing.T) {
	ts := setupVizServer(t)
	defer ts.Close()

	// Register a node
	nodeResp, err := http.Post(ts.URL+"/api/v1/nodes/join", "application/json",
		strings.NewReader(`{"node_name":"worker-1","capabilities":["gpu","coding"]}`))
	if err != nil {
		t.Fatalf("join node failed: %v", err)
	}
	nodeResp.Body.Close()

	// Submit a task
	taskResp, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"test task","requires":[]}`))
	if err != nil {
		t.Fatalf("submit task failed: %v", err)
	}
	taskResp.Body.Close()

	// Get topology
	resp, err := http.Get(ts.URL + "/api/v1/cluster/topology")
	if err != nil {
		t.Fatalf("GET /cluster/topology failed: %v", err)
	}
	defer resp.Body.Close()

	var topo struct {
		Nodes []struct {
			ID           string   `json:"id"`
			Name         string   `json:"name"`
			Status       string   `json:"status"`
			Capabilities []string `json:"capabilities"`
		} `json:"nodes"`
		Tasks []struct {
			ID     string `json:"id"`
			Title  string `json:"title"`
			Status string `json:"status"`
		} `json:"tasks"`
		Edges []struct {
			From string `json:"from"`
			To   string `json:"to"`
			Type string `json:"type"`
		} `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&topo); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Should have 2 nodes (self + worker-1)
	if len(topo.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(topo.Nodes))
	}

	// Should have 1 task
	if len(topo.Tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(topo.Tasks))
	}
	if topo.Tasks[0].Title != "test task" {
		t.Errorf("expected title 'test task', got %q", topo.Tasks[0].Title)
	}

	// Find the worker-1 node and verify capabilities
	for _, n := range topo.Nodes {
		if n.Name == "worker-1" {
			if len(n.Capabilities) != 2 {
				t.Errorf("expected 2 capabilities, got %d", len(n.Capabilities))
			}
			break
		}
	}
}

func TestClusterViz_MetricsWithTasks(t *testing.T) {
	ts := setupVizServer(t)
	defer ts.Close()

	// Submit 3 tasks
	for i := 0; i < 3; i++ {
		resp, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
			strings.NewReader(`{"title":"task","requires":[]}`))
		if err != nil {
			t.Fatalf("submit task %d failed: %v", i, err)
		}
		resp.Body.Close()
	}

	// Get task list to find IDs
	tasksResp, err := http.Get(ts.URL + "/api/v1/tasks")
	if err != nil {
		t.Fatalf("list tasks failed: %v", err)
	}
	var tasks []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	json.NewDecoder(tasksResp.Body).Decode(&tasks)
	tasksResp.Body.Close()

	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}

	// Complete one task
	completeResp, err := http.Post(ts.URL+"/api/v1/tasks/"+tasks[0].ID+"/complete", "application/json", nil)
	if err != nil {
		t.Fatalf("complete task failed: %v", err)
	}
	completeResp.Body.Close()

	// Get metrics
	resp, err := http.Get(ts.URL + "/api/v1/cluster/metrics")
	if err != nil {
		t.Fatalf("GET /cluster/metrics failed: %v", err)
	}
	defer resp.Body.Close()

	var metrics struct {
		Tasks struct {
			Total          int            `json:"total"`
			ByStatus       map[string]int `json:"by_status"`
			CompletionRate float64        `json:"completion_rate"`
		} `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if metrics.Tasks.Total != 3 {
		t.Errorf("expected 3 total tasks, got %d", metrics.Tasks.Total)
	}
	if metrics.Tasks.ByStatus["completed"] != 1 {
		t.Errorf("expected 1 completed task, got %d", metrics.Tasks.ByStatus["completed"])
	}
	// Other tasks may be ready or assigned depending on scheduler behavior
	remaining := metrics.Tasks.Total - metrics.Tasks.ByStatus["completed"]
	if remaining != 2 {
		t.Errorf("expected 2 remaining tasks, got %d", remaining)
	}
}

func TestClusterViz_CombinedEndpoint(t *testing.T) {
	ts := setupVizServer(t)
	defer ts.Close()

	// Register a node
	nodeResp, err := http.Post(ts.URL+"/api/v1/nodes/join", "application/json",
		strings.NewReader(`{"node_name":"worker-a","capabilities":["cpu"]}`))
	if err != nil {
		t.Fatalf("join node failed: %v", err)
	}
	nodeResp.Body.Close()

	// Submit a task
	taskResp, err := http.Post(ts.URL+"/api/v1/tasks", "application/json",
		strings.NewReader(`{"title":"combined task","requires":[]}`))
	if err != nil {
		t.Fatalf("submit task failed: %v", err)
	}
	taskResp.Body.Close()

	// Get combined viz
	resp, err := http.Get(ts.URL + "/api/v1/cluster/viz")
	if err != nil {
		t.Fatalf("GET /cluster/viz failed: %v", err)
	}
	defer resp.Body.Close()

	var viz struct {
		Topology struct {
			Nodes []interface{} `json:"nodes"`
			Tasks []interface{} `json:"tasks"`
		} `json:"topology"`
		Metrics struct {
			Tasks struct {
				Total int `json:"total"`
			} `json:"tasks"`
		} `json:"metrics"`
		Timeline struct {
			Events []interface{} `json:"events"`
		} `json:"timeline"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&viz); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Topology should have nodes and tasks
	if len(viz.Topology.Nodes) != 2 {
		t.Errorf("expected 2 nodes in topology, got %d", len(viz.Topology.Nodes))
	}
	if len(viz.Topology.Tasks) != 1 {
		t.Errorf("expected 1 task in topology, got %d", len(viz.Topology.Tasks))
	}

	// Metrics should reflect the task
	if viz.Metrics.Tasks.Total != 1 {
		t.Errorf("expected 1 task in metrics, got %d", viz.Metrics.Tasks.Total)
	}
}

func TestClusterViz_TimelineLimit(t *testing.T) {
	ts := setupVizServer(t)
	defer ts.Close()

	// Get timeline with limit=0 (should use default 50)
	resp, err := http.Get(ts.URL + "/api/v1/cluster/timeline?limit=0")
	if err != nil {
		t.Fatalf("GET /cluster/timeline failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Get timeline with limit=2
	resp2, err := http.Get(ts.URL + "/api/v1/cluster/timeline?limit=2")
	if err != nil {
		t.Fatalf("GET /cluster/timeline?limit=2 failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}
}

func TestClusterViz_TopologyDependencies(t *testing.T) {
	ts := setupVizServer(t)
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

	// Get topology
	resp, err := http.Get(ts.URL + "/api/v1/cluster/topology")
	if err != nil {
		t.Fatalf("GET /cluster/topology failed: %v", err)
	}
	defer resp.Body.Close()

	var topo struct {
		Tasks []struct {
			ID              string `json:"id"`
			DependencyCount int    `json:"dependency_count"`
		} `json:"tasks"`
		Edges []struct {
			From string `json:"from"`
			To   string `json:"to"`
			Type string `json:"type"`
		} `json:"edges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&topo); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Should have 2 tasks
	if len(topo.Tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(topo.Tasks))
	}

	// Task B should have 1 dependency
	for _, task := range topo.Tasks {
		if task.ID == taskB.ID {
			if task.DependencyCount != 1 {
				t.Errorf("expected task B to have 1 dependency, got %d", task.DependencyCount)
			}
		}
	}

	// Should have 1 dependency edge
	depEdgeCount := 0
	for _, edge := range topo.Edges {
		if edge.Type == "dependency" {
			depEdgeCount++
		}
	}
	if depEdgeCount != 1 {
		t.Errorf("expected 1 dependency edge, got %d", depEdgeCount)
	}
}
