package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// newTestCollector creates a collector with an isolated registry for testing.
func newTestCollector(t *testing.T) *Collector {
	t.Helper()
	reg := prometheus.NewRegistry()
	return NewWithRegistry(reg)
}

// getCounterValue extracts the total value from a prometheus.Collector used as a Counter.
// For CounterVec, sums all label combinations.
func getCounterValue(t *testing.T, c prometheus.Collector) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 20)
	c.Collect(ch)
	close(ch)
	var total float64
	for m := range ch {
		pb := &dto.Metric{}
		if err := m.Write(pb); err != nil {
			t.Fatalf("failed to write metric: %v", err)
		}
		total += pb.GetCounter().GetValue()
	}
	return total
}

// getGaugeValue extracts the value from a prometheus.Gauge.
func getGaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 1)
	g.Collect(ch)
	m := <-ch
	pb := &dto.Metric{}
	if err := m.Write(pb); err != nil {
		t.Fatalf("failed to write metric: %v", err)
	}
	return pb.GetGauge().GetValue()
}

func TestNew(t *testing.T) {
	c := newTestCollector(t)
	if c == nil {
		t.Fatal("New() returned nil")
	}
	if c.NodesTotal == nil {
		t.Error("NodesTotal is nil")
	}
	if c.TasksCreatedTotal == nil {
		t.Error("TasksCreatedTotal is nil")
	}
	if c.HTTPRequestsTotal == nil {
		t.Error("HTTPRequestsTotal is nil")
	}
}

func TestTaskCreated(t *testing.T) {
	c := newTestCollector(t)
	c.TaskCreated()
	c.TaskCreated()
	c.TaskCreated()
	if v := getCounterValue(t, c.TasksCreatedTotal); v != 3 {
		t.Errorf("expected 3, got %v", v)
	}
}

func TestTaskCompleted(t *testing.T) {
	c := newTestCollector(t)
	createdAt := time.Now().Add(-5 * time.Second)
	c.TaskCompleted(createdAt)
	if v := getCounterValue(t, c.TasksCompletedTotal); v != 1 {
		t.Errorf("expected 1, got %v", v)
	}
}

func TestTaskFailed(t *testing.T) {
	c := newTestCollector(t)
	createdAt := time.Now().Add(-10 * time.Second)
	c.TaskFailed(createdAt)
	if v := getCounterValue(t, c.TasksFailedTotal); v != 1 {
		t.Errorf("expected 1, got %v", v)
	}
}

func TestTaskScheduled(t *testing.T) {
	c := newTestCollector(t)
	readyAt := time.Now().Add(-2 * time.Second)
	c.TaskScheduled(readyAt)
	if v := getCounterValue(t, c.TasksScheduledTotal); v != 1 {
		t.Errorf("expected 1, got %v", v)
	}
}

func TestNodeRegistered(t *testing.T) {
	c := newTestCollector(t)
	c.NodeRegistered(3, 5)
	if v := getGaugeValue(t, c.NodesOnline); v != 3 {
		t.Errorf("expected 3, got %v", v)
	}
	if v := getGaugeValue(t, c.NodesTotal); v != 5 {
		t.Errorf("expected 5, got %v", v)
	}
}

func TestNodeHeartbeatReceived(t *testing.T) {
	c := newTestCollector(t)
	c.NodeHeartbeatReceived("node_a")
	c.NodeHeartbeatReceived("node_a")
	c.NodeHeartbeatReceived("node_b")
	ch := make(chan prometheus.Metric, 10)
	c.NodeHeartbeat.Collect(ch)
	close(ch)
	for m := range ch {
		pb := &dto.Metric{}
		m.Write(pb)
		for _, lp := range pb.GetLabel() {
			if lp.GetName() == "node_id" && lp.GetValue() == "node_a" {
				if v := pb.GetCounter().GetValue(); v != 2 {
					t.Errorf("node_a expected 2, got %v", v)
				}
			}
		}
	}
}

func TestLeaseCreated(t *testing.T) {
	c := newTestCollector(t)
	c.LeaseCreated(60 * time.Second)
	if v := getCounterValue(t, c.LeasesCreatedTotal); v != 1 {
		t.Errorf("expected 1, got %v", v)
	}
}

func TestLeaseExpiredAndRevoked(t *testing.T) {
	c := newTestCollector(t)
	c.LeaseExpired()
	c.LeaseExpired()
	c.LeaseRevoked()
	if v := getCounterValue(t, c.LeasesExpiredTotal); v != 2 {
		t.Errorf("expected 2, got %v", v)
	}
	if v := getCounterValue(t, c.LeasesRevokedTotal); v != 1 {
		t.Errorf("expected 1, got %v", v)
	}
}

func TestSyncVersionUpdate(t *testing.T) {
	c := newTestCollector(t)
	c.SyncVersionUpdate(42)
	if v := getGaugeValue(t, c.SyncVersion); v != 42 {
		t.Errorf("expected 42, got %v", v)
	}
}

func TestRecoveryEvent(t *testing.T) {
	c := newTestCollector(t)
	c.RecoveryEvent("full_recovery", "completed")
	c.RecoveryEvent("full_recovery", "partial")
	if v := getCounterValue(t, c.RecoveryEventsTotal); v != 2 {
		t.Errorf("expected 2, got %v", v)
	}
}

func TestWorkflowTriggersAndResolutions(t *testing.T) {
	c := newTestCollector(t)
	c.WorkflowTrigger()
	c.WorkflowTrigger()
	c.WorkflowResolution()
	if v := getCounterValue(t, c.WorkflowTriggersTotal); v != 2 {
		t.Errorf("expected 2, got %v", v)
	}
	if v := getCounterValue(t, c.WorkflowResolutionsTotal); v != 1 {
		t.Errorf("expected 1, got %v", v)
	}
}

func TestHTTPMiddleware(t *testing.T) {
	c := newTestCollector(t)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	wrapped := Middleware(c)(handler)
	req := httptest.NewRequest("GET", "/api/v1/tasks", nil)
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	if v := getCounterValue(t, c.HTTPRequestsTotal); v != 1 {
		t.Errorf("expected 1 request recorded, got %v", v)
	}
}

func TestHTTPMiddlewareErrorStatus(t *testing.T) {
	c := newTestCollector(t)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	wrapped := Middleware(c)(handler)
	req := httptest.NewRequest("POST", "/api/v1/tasks", nil)
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rr.Code)
	}
}

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/api/v1/tasks", "/api/v1/tasks"},
		{"/api/v1/tasks/task_a1b2c3d4e5f6", "/api/v1/tasks/task_{id}"},
		{"/api/v1/leases/lease_task123_node456", "/api/v1/leases/lease_{id}"},
		{"/api/v1/nodes/node_abc123def456", "/api/v1/nodes/node_{id}"},
		{"/api/v1/tasks/550e8400-e29b-41d4-a716-446655440000", "/api/v1/tasks/{id}"},
		{"/api/v1/nodes/12345", "/api/v1/nodes/{id}"},
	}
	for _, tt := range tests {
		got := normalizePath(tt.input)
		if got != tt.want {
			t.Errorf("normalizePath(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestIsUUID(t *testing.T) {
	if !isUUID("550e8400-e29b-41d4-a716-446655440000") {
		t.Error("valid UUID not recognized")
	}
	if isUUID("not-a-uuid") {
		t.Error("invalid UUID accepted")
	}
	if isUUID("550e8400e29b41d4a716446655440000") {
		t.Error("UUID without dashes accepted")
	}
}
