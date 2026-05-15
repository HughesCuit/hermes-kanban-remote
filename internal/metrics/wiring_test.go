package metrics

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

// --- Additional helpers (getCounterValue, getGaugeValue are in collector_test.go) ---

func getHistogramCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 10)
	h.Collect(ch)
	close(ch)
	for m := range ch {
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("failed to write metric: %v", err)
		}
		return pb.GetHistogram().GetSampleCount()
	}
	return 0
}

func getHistogramSum(t *testing.T, h prometheus.Histogram) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 10)
	h.Collect(ch)
	close(ch)
	for m := range ch {
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("failed to write metric: %v", err)
		}
		return pb.GetHistogram().GetSampleSum()
	}
	return 0
}

func getGaugeVecValue(t *testing.T, gv prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	m, err := gv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("failed to get metric: %v", err)
	}
	pb := &dto.Metric{}
	if err := m.Write(pb); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	return pb.GetGauge().GetValue()
}

// getHistogramVecCount sums sample counts across all label combinations in a HistogramVec.
func getHistogramVecCount(t *testing.T, c prometheus.Collector) uint64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 50)
	c.Collect(ch)
	close(ch)
	var total uint64
	for m := range ch {
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("failed to write metric: %v", err)
		}
		total += pb.GetHistogram().GetSampleCount()
	}
	return total
}

// --- Integration tests for metrics wiring ---

func TestMetricsWiring_TaskLifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	// Simulate task creation
	c.TaskCreated()
	c.TaskCreated()
	c.TaskCreated()

	if v := getCounterValue(t, c.TasksCreatedTotal); v != 3 {
		t.Errorf("TasksCreatedTotal: expected 3, got %v", v)
	}

	// Simulate task completion with 5s duration
	createdAt := time.Now().Add(-5 * time.Second)
	c.TaskCompleted(createdAt)
	c.TaskCompleted(createdAt)

	if v := getCounterValue(t, c.TasksCompletedTotal); v != 2 {
		t.Errorf("TasksCompletedTotal: expected 2, got %v", v)
	}

	// Verify task duration histogram
	if count := getHistogramCount(t, c.TaskDuration); count != 2 {
		t.Errorf("TaskDuration count: expected 2, got %v", count)
	}
	if sum := getHistogramSum(t, c.TaskDuration); sum < 9.9 {
		t.Errorf("TaskDuration sum: expected ~10s, got %v", sum)
	}

	// Simulate task failure
	createdAt2 := time.Now().Add(-10 * time.Second)
	c.TaskFailed(createdAt2)

	if v := getCounterValue(t, c.TasksFailedTotal); v != 1 {
		t.Errorf("TasksFailedTotal: expected 1, got %v", v)
	}

	// Verify task status gauges
	c.TaskStatusUpdate("ready", 2)
	c.TaskStatusUpdate("completed", 2)
	c.TaskStatusUpdate("failed", 1)

	if v := getGaugeVecValue(t, c.TasksTotal, "ready"); v != 2 {
		t.Errorf("TasksTotal[ready]: expected 2, got %v", v)
	}
	if v := getGaugeVecValue(t, c.TasksTotal, "completed"); v != 2 {
		t.Errorf("TasksTotal[completed]: expected 2, got %v", v)
	}
	if v := getGaugeVecValue(t, c.TasksTotal, "failed"); v != 1 {
		t.Errorf("TasksTotal[failed]: expected 1, got %v", v)
	}
}

func TestMetricsWiring_NodeLifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	c.NodeRegistered(3, 5)
	if v := getGaugeValue(t, c.NodesOnline); v != 3 {
		t.Errorf("NodesOnline: expected 3, got %v", v)
	}
	if v := getGaugeValue(t, c.NodesTotal); v != 5 {
		t.Errorf("NodesTotal: expected 5, got %v", v)
	}

	c.NodeRegistered(4, 6)
	if v := getGaugeValue(t, c.NodesOnline); v != 4 {
		t.Errorf("NodesOnline after update: expected 4, got %v", v)
	}

	// Simulate heartbeats
	c.NodeHeartbeatReceived("node_a")
	c.NodeHeartbeatReceived("node_a")
	c.NodeHeartbeatReceived("node_b")

	if v := getCounterValue(t, c.NodeHeartbeat); v != 3 {
		t.Errorf("NodeHeartbeat total: expected 3, got %v", v)
	}
}

func TestMetricsWiring_LeaseLifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	c.LeaseCreated(60 * time.Second)
	c.LeaseCreated(120 * time.Second)

	if v := getCounterValue(t, c.LeasesCreatedTotal); v != 2 {
		t.Errorf("LeasesCreatedTotal: expected 2, got %v", v)
	}

	if count := getHistogramCount(t, c.LeaseDuration); count != 2 {
		t.Errorf("LeaseDuration count: expected 2, got %v", count)
	}

	c.LeasesActiveUpdate(3)
	if v := getGaugeValue(t, c.LeasesActive); v != 3 {
		t.Errorf("LeasesActive: expected 3, got %v", v)
	}

	c.LeaseExpired()
	if v := getCounterValue(t, c.LeasesExpiredTotal); v != 1 {
		t.Errorf("LeasesExpiredTotal: expected 1, got %v", v)
	}

	c.LeaseRevoked()
	if v := getCounterValue(t, c.LeasesRevokedTotal); v != 1 {
		t.Errorf("LeasesRevokedTotal: expected 1, got %v", v)
	}

	c.LeasesActiveUpdate(1)
	if v := getGaugeValue(t, c.LeasesActive); v != 1 {
		t.Errorf("LeasesActive after expiry: expected 1, got %v", v)
	}
}

func TestMetricsWiring_RecoveryAndSync(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	c.RecoveryEvent("trigger", "accepted")
	c.RecoveryEvent("full_recovery", "completed")
	c.RecoveryEvent("full_recovery", "partial")

	if v := getCounterValue(t, c.RecoveryEventsTotal); v != 3 {
		t.Errorf("RecoveryEventsTotal: expected 3, got %v", v)
	}

	c.RecoveryReschedule()
	if v := getCounterValue(t, c.RecoveryTasksRescheduled); v != 1 {
		t.Errorf("RecoveryTasksRescheduled: expected 1, got %v", v)
	}

	c.SyncVersionUpdate(42)
	if v := getGaugeValue(t, c.SyncVersion); v != 42 {
		t.Errorf("SyncVersion: expected 42, got %v", v)
	}

	c.SyncVersionUpdate(100)
	if v := getGaugeValue(t, c.SyncVersion); v != 100 {
		t.Errorf("SyncVersion after update: expected 100, got %v", v)
	}
}

func TestMetricsWiring_Workflow(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	c.WorkflowTrigger()
	c.WorkflowTrigger()
	c.WorkflowTrigger()

	if v := getCounterValue(t, c.WorkflowTriggersTotal); v != 3 {
		t.Errorf("WorkflowTriggersTotal: expected 3, got %v", v)
	}

	c.WorkflowResolution()
	c.WorkflowResolution()

	if v := getCounterValue(t, c.WorkflowResolutionsTotal); v != 2 {
		t.Errorf("WorkflowResolutionsTotal: expected 2, got %v", v)
	}
}

func TestMetricsWiring_Scheduling(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	readyAt1 := time.Now().Add(-2 * time.Second)
	readyAt2 := time.Now().Add(-5 * time.Second)
	c.TaskScheduled(readyAt1)
	c.TaskScheduled(readyAt2)

	if v := getCounterValue(t, c.TasksScheduledTotal); v != 2 {
		t.Errorf("TasksScheduledTotal: expected 2, got %v", v)
	}

	if count := getHistogramCount(t, c.ScheduleDuration); count != 2 {
		t.Errorf("ScheduleDuration count: expected 2, got %v", count)
	}
	if sum := getHistogramSum(t, c.ScheduleDuration); sum < 6.9 {
		t.Errorf("ScheduleDuration sum: expected ~7s, got %v", sum)
	}
}

func TestMetricsWiring_HTTPMiddlewareEndToEnd(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/tasks":
			if r.Method == "POST" {
				w.WriteHeader(http.StatusCreated)
				json.NewEncoder(w).Encode(map[string]string{"id": "task_123"})
			} else {
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode([]string{"task_123"})
			}
		case r.URL.Path == "/api/v1/nodes":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode([]string{"node_1"})
		case strings.HasPrefix(r.URL.Path, "/api/v1/tasks/"):
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"id": "task_abc123"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	wrapped := Middleware(c)(handler)

	reqs := []struct {
		method string
		path   string
		code   int
	}{
		{"POST", "/api/v1/tasks", http.StatusCreated},
		{"GET", "/api/v1/tasks", http.StatusOK},
		{"GET", "/api/v1/nodes", http.StatusOK},
		{"GET", "/api/v1/tasks/task_abc123", http.StatusOK},
		{"POST", "/api/v1/tasks", http.StatusCreated},
	}

	for _, req := range reqs {
		r := httptest.NewRequest(req.method, req.path, nil)
		rr := httptest.NewRecorder()
		wrapped.ServeHTTP(rr, r)
		if rr.Code != req.code {
			t.Errorf("%s %s: expected %d, got %d", req.method, req.path, req.code, rr.Code)
		}
	}

	if v := getCounterValue(t, c.HTTPRequestsTotal); v != 5 {
		t.Errorf("HTTPRequestsTotal: expected 5, got %v", v)
	}
}

func TestMetricsWiring_NilSafe(t *testing.T) {
	c := NewWithRegistry(prometheus.NewRegistry())

	c.TaskCreated()
	c.TaskCompleted(time.Now())
	c.TaskFailed(time.Now())
	c.TaskScheduled(time.Now())
	c.NodeRegistered(1, 1)
	c.NodeHeartbeatReceived("node_1")
	c.LeaseCreated(60 * time.Second)
	c.LeaseExpired()
	c.LeaseRevoked()
	c.LeasesActiveUpdate(1)
	c.SyncVersionUpdate(1)
	c.RecoveryEvent("test", "ok")
	c.RecoveryReschedule()
	c.WorkflowTrigger()
	c.WorkflowResolution()
	c.TaskStatusUpdate("ready", 1)
}

func TestMetricsWriting_PrometheusFormat(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	c.TaskCreated()
	c.TaskCompleted(time.Now())
	c.NodeRegistered(2, 3)
	c.LeaseCreated(60 * time.Second)
	c.RecoveryEvent("trigger", "ok")
	c.WorkflowTrigger()
	c.SyncVersionUpdate(5)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	expectedMetrics := map[string]bool{
		"hac_tasks_created_total":     false,
		"hac_tasks_completed_total":   false,
		"hac_nodes_online":            false,
		"hac_nodes_total":             false,
		"hac_leases_created_total":    false,
		"hac_recovery_events_total":   false,
		"hac_workflow_triggers_total": false,
		"hac_sync_version":            false,
		"hac_task_duration_seconds":   false,
	}

	for _, f := range families {
		name := f.GetName()
		if _, ok := expectedMetrics[name]; ok {
			expectedMetrics[name] = true
		}
	}

	for name, found := range expectedMetrics {
		if !found {
			t.Errorf("metric %q not found in /metrics output", name)
		}
	}
}

func TestMetricsWiring_ConcurrentAccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				c.TaskCreated()
				c.NodeHeartbeatReceived("node_a")
				c.LeaseCreated(60 * time.Second)
			}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if v := getCounterValue(t, c.TasksCreatedTotal); v != 1000 {
		t.Errorf("TasksCreatedTotal: expected 1000, got %v", v)
	}
	if v := getCounterValue(t, c.LeasesCreatedTotal); v != 1000 {
		t.Errorf("LeasesCreatedTotal: expected 1000, got %v", v)
	}
}

func TestMetricsFormat_Payload(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := NewWithRegistry(reg)

	c.TaskCreated()
	c.NodeRegistered(1, 1)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("metrics endpoint returned %d", rr.Code)
	}

	body := rr.Body.String()
	for _, want := range []string{
		"hac_tasks_created_total 1",
		"hac_nodes_online 1",
		"hac_nodes_total 1",
	} {
		if !bytes.Contains([]byte(body), []byte(want)) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}
