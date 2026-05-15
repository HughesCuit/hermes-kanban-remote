package federation

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// --- Registry tests ---

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	c := r.Register("c1", "cluster-a", "http://localhost:8080")
	if c == nil {
		t.Fatal("expected non-nil cluster")
	}
	if c.ID != "c1" || c.Name != "cluster-a" {
		t.Fatalf("unexpected cluster: %+v", c)
	}
	if c.Status != ClusterAvailable {
		t.Fatalf("expected available, got %s", c.Status)
	}

	got, ok := r.Get("c1")
	if !ok {
		t.Fatal("expected to find c1")
	}
	if got.Endpoint != "http://localhost:8080" {
		t.Fatalf("unexpected endpoint: %s", got.Endpoint)
	}
}

func TestRegistryUpdate(t *testing.T) {
	r := NewRegistry()
	r.Register("c1", "old-name", "http://old:8080")
	r.Register("c1", "new-name", "http://new:8080")

	got, _ := r.Get("c1")
	if got.Name != "new-name" || got.Endpoint != "http://new:8080" {
		t.Fatalf("update failed: %+v", got)
	}
}

func TestRegistryRemove(t *testing.T) {
	r := NewRegistry()
	r.Register("c1", "cluster-a", "http://localhost:8080")

	if !r.Remove("c1") {
		t.Fatal("expected remove to return true")
	}
	if _, ok := r.Get("c1"); ok {
		t.Fatal("expected c1 to be removed")
	}
	if r.Remove("c1") {
		t.Fatal("expected remove to return false for non-existent")
	}
}

func TestRegistryCount(t *testing.T) {
	r := NewRegistry()
	if r.Count() != 0 {
		t.Fatal("expected 0")
	}
	r.Register("c1", "a", "http://a:8080")
	r.Register("c2", "b", "http://b:8080")
	if r.Count() != 2 {
		t.Fatalf("expected 2, got %d", r.Count())
	}
	if r.AvailableCount() != 2 {
		t.Fatalf("expected 2 available, got %d", r.AvailableCount())
	}

	r.MarkUnavailable("c1")
	if r.AvailableCount() != 1 {
		t.Fatalf("expected 1 available, got %d", r.AvailableCount())
	}

	r.MarkAvailable("c1", 5*time.Millisecond)
	if r.AvailableCount() != 2 {
		t.Fatalf("expected 2 available after mark, got %d", r.AvailableCount())
	}
}

func TestRegistryGetAll(t *testing.T) {
	r := NewRegistry()
	r.Register("c1", "a", "http://a:8080")
	r.Register("c2", "b", "http://b:8080")

	all := r.GetAll()
	if len(all) != 2 {
		t.Fatalf("expected 2, got %d", len(all))
	}
}

// --- Client tests ---

func TestClientPing(t *testing.T) {
	// Mock remote cluster that returns a status response
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"entries": []map[string]string{},
			"summary": map[string]int{
				"total_nodes":   3,
				"online_nodes":  2,
				"total_tasks":   10,
				"running_tasks": 5,
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewClient()
	ping, latency, err := client.Ping(server.URL)
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if latency <= 0 {
		t.Fatal("expected positive latency")
	}
	if ping.Summary.TotalNodes != 3 {
		t.Fatalf("expected 3 total nodes, got %d", ping.Summary.TotalNodes)
	}
}

func TestClientPingUnavailable(t *testing.T) {
	client := NewClient()
	_, _, err := client.Ping("http://localhost:19999")
	if err == nil {
		t.Fatal("expected error for unreachable cluster")
	}
}

func TestClientForwardTask(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		var req ForwardTaskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"id":     "remote_task_123",
			"title":  req.Title,
			"status": "ready",
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewClient()
	resp, err := client.ForwardTask(server.URL, "test task", []string{"python"})
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	if resp.ID != "remote_task_123" {
		t.Fatalf("expected remote_task_123, got %s", resp.ID)
	}
}

func TestClientQueryStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"entries": []map[string]string{
				{"node_id": "n1", "status": "online"},
			},
			"summary": map[string]int{
				"total_nodes":  1,
				"online_nodes": 1,
			},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewClient()
	status, err := client.QueryStatus(server.URL)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(status.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(status.Entries))
	}
}

// --- Dispatcher tests ---

func TestDispatcherForwardTask(t *testing.T) {
	// Set up mock remote cluster
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"id":     "remote_abc",
			"title":  "forwarded",
			"status": "ready",
		})
	})
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"entries": []map[string]string{},
			"summary": map[string]int{"total_nodes": 1, "online_nodes": 1},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	registry := NewRegistry()
	registry.Register("c1", "remote-a", server.URL)
	client := NewClient()
	dispatcher := NewDispatcher(registry, client)

	remoteID, err := dispatcher.ForwardTask("c1", "test task", []string{"go"})
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	if remoteID != "remote_abc" {
		t.Fatalf("expected remote_abc, got %s", remoteID)
	}
}

func TestDispatcherForwardTaskNotFound(t *testing.T) {
	registry := NewRegistry()
	client := NewClient()
	dispatcher := NewDispatcher(registry, client)

	_, err := dispatcher.ForwardTask("nonexistent", "test", nil)
	if err != ErrClusterNotFound {
		t.Fatalf("expected ErrClusterNotFound, got %v", err)
	}
}

func TestDispatcherForwardTaskUnavailable(t *testing.T) {
	registry := NewRegistry()
	registry.Register("c1", "dead-cluster", "http://localhost:19999")
	registry.MarkUnavailable("c1")
	client := NewClient()
	dispatcher := NewDispatcher(registry, client)

	_, err := dispatcher.ForwardTask("c1", "test", nil)
	if err != ErrClusterUnavailable {
		t.Fatalf("expected ErrClusterUnavailable, got %v", err)
	}
}

func TestDispatcherQueryStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"entries": []map[string]string{},
			"summary": map[string]int{"total_nodes": 5, "online_nodes": 3},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	registry := NewRegistry()
	registry.Register("c1", "remote-a", server.URL)
	client := NewClient()
	dispatcher := NewDispatcher(registry, client)

	status, err := dispatcher.QueryClusterStatus("c1")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if status.Summary.TotalNodes != 5 {
		t.Fatalf("expected 5 total nodes, got %d", status.Summary.TotalNodes)
	}
}

func TestDispatcherHealthCheck(t *testing.T) {
	// Server that responds to ping
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"entries": []map[string]string{},
			"summary": map[string]int{},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	registry := NewRegistry()
	registry.Register("c1", "healthy", server.URL)
	client := NewClient()
	dispatcher := NewDispatcher(registry, client)

	// Start with fast interval for test
	dispatcher.Start(50 * time.Millisecond)
	defer dispatcher.Stop()

	// Wait for at least one ping cycle
	time.Sleep(150 * time.Millisecond)

	c, ok := registry.Get("c1")
	if !ok {
		t.Fatal("cluster should exist")
	}
	if c.Status != ClusterAvailable {
		t.Fatalf("expected available after ping, got %s", c.Status)
	}
	if c.PingLatency <= 0 {
		t.Fatal("expected positive ping latency")
	}
}

func TestDispatcherHealthCheckUnavailable(t *testing.T) {
	registry := NewRegistry()
	registry.Register("c1", "dead", "http://localhost:19999")
	client := NewClient()
	dispatcher := NewDispatcher(registry, client)

	dispatcher.Start(50 * time.Millisecond)
	defer dispatcher.Stop()

	time.Sleep(150 * time.Millisecond)

	c, ok := registry.Get("c1")
	if !ok {
		t.Fatal("cluster should exist")
	}
	if c.Status != ClusterUnavailable {
		t.Fatalf("expected unavailable after failed ping, got %s", c.Status)
	}
}
