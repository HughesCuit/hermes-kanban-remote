package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/dashboard"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/metrics"
	"github.com/heventure/hermes-agent-cluster/internal/recovery"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
	"github.com/heventure/hermes-agent-cluster/internal/status"
	"github.com/heventure/hermes-agent-cluster/internal/sync"
	"github.com/heventure/hermes-agent-cluster/internal/telemetry"
	"github.com/heventure/hermes-agent-cluster/internal/visualization"
	"github.com/heventure/hermes-agent-cluster/internal/workflow"
)

const maxBodySize = 1 << 20 // 1MB

// Server holds all dependencies for the API.
type Server struct {
	Router      *chi.Mux
	Registry    *cluster.Registry
	Scheduler   *scheduler.Scheduler
	LeaseMgr    *lease.Manager
	Recovery    *recovery.Detector
	Log         *recovery.Log
	StateStore  *sync.StateStore
	Receiver    *sync.FollowerReceiver
	LeaderSync  *sync.LeaderSync
	StatusView  *status.StatusView
	Resolver    *workflow.Resolver
	ClusterView *visualization.ClusterView
	Telemetry   *telemetry.Metrics
	PromMetrics *metrics.Collector
}

// ServerOption configures optional Server fields.
type ServerOption func(*Server)

// WithClusterView sets the ClusterView for visualization endpoints.
func WithClusterView(cv *visualization.ClusterView) ServerOption {
	return func(s *Server) { s.ClusterView = cv }
}

// WithTelemetry sets the Telemetry metrics and installs telemetry middleware.
func WithTelemetry(m *telemetry.Metrics) ServerOption {
	return func(s *Server) { s.Telemetry = m }
}

// WithPromMetrics sets the Prometheus metrics collector and installs metrics middleware.
func WithPromMetrics(c *metrics.Collector) ServerOption {
	return func(s *Server) { s.PromMetrics = c }
}

// NewServer creates a new API server.
func NewServer(
	registry *cluster.Registry,
	sched *scheduler.Scheduler,
	leaseMgr *lease.Manager,
	detector *recovery.Detector,
	recLog *recovery.Log,
	stateStore *sync.StateStore,
	receiver *sync.FollowerReceiver,
	leaderSync *sync.LeaderSync,
	resolver *workflow.Resolver,
	opts ...ServerOption,
) *Server {
	sv := status.NewStatusView(registry, sched.GetTaskStore(), leaseMgr)
	s := &Server{
		Router:     chi.NewRouter(),
		Registry:   registry,
		Scheduler:  sched,
		LeaseMgr:   leaseMgr,
		Recovery:   detector,
		Log:        recLog,
		StateStore: stateStore,
		Receiver:   receiver,
		LeaderSync: leaderSync,
		StatusView: sv,
		Resolver:   resolver,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.Router.Use(middleware.Logger)
	s.Router.Use(middleware.Recoverer)
	if s.Telemetry != nil {
		s.Router.Use(telemetry.Middleware(s.Telemetry))
	}
	// Prometheus HTTP request metrics middleware
	if s.PromMetrics != nil {
		s.Router.Use(metrics.Middleware(s.PromMetrics))
	}
	s.setupRoutes()
	return s
}

func (s *Server) setupRoutes() {
	s.Router.Route("/api/v1", func(r chi.Router) {
		// Node management
		r.Post("/nodes/join", s.handleJoin)
		r.Post("/nodes/heartbeat", s.handleHeartbeat)
		r.Get("/nodes", s.handleListNodes)
		r.Patch("/nodes/{id}/capabilities", s.handleUpdateCapabilities)

		// Task management
		r.Post("/tasks", s.handleSubmitTask)
		r.Get("/tasks", s.handleListTasks)
		r.Post("/tasks/{id}/complete", s.handleCompleteTask)
		r.Post("/tasks/{id}/fail", s.handleFailTask)
		r.Post("/tasks/{id}/unblock", s.handleUnblockTask)
		r.Post("/tasks/{id}/advance", s.handleManualAdvance)

		// Lease management
		r.Post("/leases", s.handleCreateLease)
		r.Delete("/leases/{id}", s.handleRevokeLease)
		r.Get("/leases", s.handleListLeases)

		// Sync
		r.Post("/sync/receive", s.handleSyncReceive)
		r.Get("/sync/status", s.handleSyncStatus)

		// Recovery
		r.Post("/recovery/trigger", s.handleRecoveryTrigger)
		r.Get("/recovery/log", s.handleRecoveryLog)
		r.Get("/recovery/stats", s.handleRecoveryStats)

		// Schedule trigger
		r.Post("/schedule/trigger", s.handleScheduleTrigger)

		// Workflow / Dependencies
		r.Post("/tasks/{id}/dependencies", s.handleSetDependencies)
		r.Get("/tasks/{id}/dependents", s.handleGetDependents)
		r.Get("/tasks/{id}/trigger-chain", s.handleGetTriggerChain)
		r.Get("/workflow/graph", s.handleGetGraph)

		// Global status view
		r.Get("/status", s.handleStatus)

		// Cluster visualization
		r.Get("/cluster/topology", s.handleClusterTopology)
		r.Get("/cluster/metrics", s.handleClusterMetrics)
		r.Get("/cluster/timeline", s.handleClusterTimeline)
		r.Get("/cluster/viz", s.handleClusterViz)
	})

	// Prometheus metrics endpoint (outside /api/v1 to avoid auth middleware)
	s.Router.Handle("/metrics", promhttp.Handler())

	// Serve the Web Dashboard at /dashboard/
	s.Router.Handle("/dashboard/*", http.StripPrefix("/dashboard/", dashboard.Handler()))
	// Redirect bare /dashboard to /dashboard/
	s.Router.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
	})
}

// --- Helpers ---

// limitBody caps the request body to maxBodySize to prevent abuse.
func limitBody(w http.ResponseWriter, r *http.Request) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)
	return true
}

// writeJSON encodes v as JSON and returns false if encoding fails.
func writeJSON(w http.ResponseWriter, v interface{}) bool {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("json encode error: %v", err)
		return false
	}
	return true
}

// --- Node handlers ---

type joinRequest struct {
	NodeName     string   `json:"node_name"`
	Capabilities []string `json:"capabilities"`
	Endpoint     string   `json:"endpoint"`
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	var req joinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	nodeID := "node_" + req.NodeName
	node := &cluster.Node{
		ID:           nodeID,
		Name:         req.NodeName,
		Capabilities: req.Capabilities,
	}
	s.Registry.Register(node)

	// Register follower URL for state sync
	if req.Endpoint != "" && s.LeaderSync != nil {
		s.LeaderSync.AddFollower(req.Endpoint)
		log.Printf("registered follower: node=%s endpoint=%s", nodeID, req.Endpoint)
	}

	writeJSON(w, map[string]string{"node_id": nodeID, "status": "registered"})

	// Prometheus: record node registration
	s.updateNodeGauges()
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	s.Registry.UpdateHeartbeat(req.NodeID)
	writeJSON(w, map[string]string{"status": "ok"})

	// Prometheus: record heartbeat + update node gauges
	if s.PromMetrics != nil {
		s.PromMetrics.NodeHeartbeatReceived(req.NodeID)
	}
	s.updateNodeGauges()
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	nodes := s.Registry.GetAll()
	writeJSON(w, nodes)
}

type updateCapabilitiesRequest struct {
	Capabilities []string `json:"capabilities"`
}

func (s *Server) handleUpdateCapabilities(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	nodeID := chi.URLParam(r, "id")
	var req updateCapabilitiesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	// Verify node exists
	if _, ok := s.Registry.Get(nodeID); !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	s.Registry.UpdateCapabilities(nodeID, req.Capabilities)
	// Re-trigger scheduling: capability changes may unlock pending tasks
	s.Scheduler.TriggerPendingTasks()
	s.Scheduler.SchedulePending()
	writeJSON(w, map[string]interface{}{
		"node_id":      nodeID,
		"capabilities": req.Capabilities,
		"status":       "updated",
	})
}

// --- Task handlers ---

type submitTaskRequest struct {
	Title    string   `json:"title"`
	Requires []string `json:"requires"`
}

func (s *Server) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	var req submitTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	taskID := scheduler.GenerateID()
	task, err := s.Scheduler.GetTaskStore().Create(taskID, req.Title, req.Requires)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	// Try to schedule immediately: trigger pending tasks first, then schedule
	s.Scheduler.TriggerPendingTasks()
	s.Scheduler.SchedulePending()
	writeJSON(w, task)

	// Prometheus: record task creation + update gauges
	s.PromMetrics.TaskCreated()
	s.updateTaskGauges()
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	tasks := s.Scheduler.GetTaskStore().GetAll()
	writeJSON(w, tasks)
}

func (s *Server) handleCompleteTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	// Read task before status change to capture CreatedAt for duration metric
	task, _ := s.Scheduler.GetTaskStore().Get(taskID)
	s.Scheduler.GetTaskStore().SetStatus(taskID, scheduler.TaskCompleted)
	// Auto-transition downstream tasks whose dependencies are now met
	if s.Resolver != nil {
		s.Resolver.OnDependencyComplete(taskID)
	}
	writeJSON(w, map[string]string{"status": "completed"})

	// Prometheus: record task completion + update gauges
	if s.PromMetrics != nil && task != nil {
		s.PromMetrics.TaskCompleted(task.CreatedAt)
	}
	s.updateTaskGauges()
}

// --- Task Failure handler ---

type failTaskRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) handleFailTask(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	taskID := chi.URLParam(r, "id")
	var req failTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Allow empty body for simple failure
		req.Reason = "failed"
	}
	store := s.Scheduler.GetTaskStore()
	// Read task before status change to capture CreatedAt for duration metric
	task, _ := store.Get(taskID)
	if err := store.SetStatus(taskID, scheduler.TaskFailed); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	// Block downstream tasks that depend on this failed task
	var blocked []string
	if s.Resolver != nil {
		blocked = s.Resolver.OnDependencyFailed(taskID)
	}
	writeJSON(w, map[string]interface{}{
		"status":  "failed",
		"blocked": blocked,
	})

	// Prometheus: record task failure + update gauges
	if s.PromMetrics != nil && task != nil {
		s.PromMetrics.TaskFailed(task.CreatedAt)
	}
	s.updateTaskGauges()
}

// --- Task Unblock handler ---

func (s *Server) handleUnblockTask(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	store := s.Scheduler.GetTaskStore()
	if err := store.Unblock(taskID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]string{"status": "unblocked"})
}

// --- Manual Advance handler ---

func (s *Server) handleManualAdvance(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	if s.Resolver == nil {
		http.Error(w, "workflow resolver not configured", http.StatusInternalServerError)
		return
	}
	if err := s.Resolver.ManualAdvance(taskID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]string{"status": "advanced"})
}

// --- Lease handlers ---

func (s *Server) handleCreateLease(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	var req struct {
		TaskID string `json:"task_id"`
		NodeID string `json:"node_id"`
		TTL    int    `json:"ttl_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	l, err := s.LeaseMgr.Create(req.TaskID, req.NodeID, time.Duration(req.TTL)*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, l)

	// Prometheus: record lease creation + update active count
	if s.PromMetrics != nil {
		s.PromMetrics.LeaseCreated(time.Duration(req.TTL) * time.Second)
		s.updateLeaseGauges()
	}
}

func (s *Server) handleRevokeLease(w http.ResponseWriter, r *http.Request) {
	leaseID := chi.URLParam(r, "id")
	if err := s.LeaseMgr.Revoke(leaseID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]string{"status": "revoked"})

	// Prometheus: record lease revocation + update active count
	if s.PromMetrics != nil {
		s.PromMetrics.LeaseRevoked()
		s.updateLeaseGauges()
	}
}

func (s *Server) handleListLeases(w http.ResponseWriter, r *http.Request) {
	leases := s.LeaseMgr.GetActiveLeases()
	writeJSON(w, leases)
}

// --- Sync handlers ---

func (s *Server) handleSyncReceive(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	var msg sync.SyncMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	applied := s.Receiver.HandleSyncMessage(msg)
	writeJSON(w, map[string]bool{"applied": applied})

	// Prometheus: update sync version gauge
	s.updateSyncGauge()
}

func (s *Server) handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]int64{"version": s.StateStore.Version()})
}

// --- Schedule trigger handler ---

func (s *Server) handleScheduleTrigger(w http.ResponseWriter, r *http.Request) {
	promoted := s.Scheduler.TriggerPendingTasks()
	scheduled := s.Scheduler.SchedulePending()
	writeJSON(w, map[string]interface{}{
		"promoted":  promoted,
		"scheduled": scheduled,
	})
}

// --- Recovery handlers ---

func (s *Server) handleRecoveryTrigger(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	var req struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	go s.Recovery.NotifyOffline(req.NodeID)
	w.WriteHeader(http.StatusAccepted)
	writeJSON(w, map[string]string{"status": "accepted"})
}

func (s *Server) handleRecoveryLog(w http.ResponseWriter, r *http.Request) {
	events := s.Log.GetEvents()
	writeJSON(w, events)
}

func (s *Server) handleRecoveryStats(w http.ResponseWriter, r *http.Request) {
	stats := s.Log.Stats()
	writeJSON(w, stats)
}

// --- Workflow / Dependency handlers ---

type setDepsRequest struct {
	DependsOn []string `json:"depends_on"`
}

func (s *Server) handleSetDependencies(w http.ResponseWriter, r *http.Request) {
	limitBody(w, r)
	taskID := chi.URLParam(r, "id")
	var req setDepsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if s.Resolver == nil {
		http.Error(w, "workflow resolver not configured", http.StatusInternalServerError)
		return
	}
	if err := s.Resolver.SetDependencies(taskID, req.DependsOn); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	task, ok := s.Scheduler.GetTaskStore().Get(taskID)
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}
	writeJSON(w, task)
}

func (s *Server) handleGetDependents(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	if s.Resolver == nil {
		http.Error(w, "workflow resolver not configured", http.StatusInternalServerError)
		return
	}
	dependents := s.Resolver.GetDependents(taskID)
	writeJSON(w, map[string]interface{}{
		"task_id":     taskID,
		"dependents":  dependents,
		"count":       len(dependents),
	})
}

func (s *Server) handleGetTriggerChain(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")
	if s.Resolver == nil {
		http.Error(w, "workflow resolver not configured", http.StatusInternalServerError)
		return
	}
	chain := s.Resolver.GetTriggerChain(taskID)
	writeJSON(w, map[string]interface{}{
		"task_id": taskID,
		"chain":   chain,
		"count":   len(chain),
	})
}

func (s *Server) handleGetGraph(w http.ResponseWriter, r *http.Request) {
	if s.Resolver == nil {
		http.Error(w, "workflow resolver not configured", http.StatusInternalServerError)
		return
	}
	graph := s.Resolver.GetGraph()
	writeJSON(w, graph)
}

// --- Cluster Visualization handlers ---

const maxLimit = 1000
const defaultLimit = 50

// parseLimit extracts and caps a "limit" query parameter from the request.
func parseLimit(r *http.Request) int {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return defaultLimit
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultLimit
	}
	if n > maxLimit {
		n = maxLimit
	}
	return n
}

func (s *Server) handleClusterTopology(w http.ResponseWriter, r *http.Request) {
	if s.ClusterView == nil {
		http.Error(w, "cluster visualization not configured", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, s.ClusterView.GetTopology())
}

func (s *Server) handleClusterMetrics(w http.ResponseWriter, r *http.Request) {
	if s.ClusterView == nil {
		http.Error(w, "cluster visualization not configured", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, s.ClusterView.GetMetrics())
}

func (s *Server) handleClusterTimeline(w http.ResponseWriter, r *http.Request) {
	if s.ClusterView == nil {
		http.Error(w, "cluster visualization not configured", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, s.ClusterView.GetTimeline(parseLimit(r)))
}

func (s *Server) handleClusterViz(w http.ResponseWriter, r *http.Request) {
	if s.ClusterView == nil {
		http.Error(w, "cluster visualization not configured", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]interface{}{
		"topology": s.ClusterView.GetTopology(),
		"metrics":  s.ClusterView.GetMetrics(),
		"timeline": s.ClusterView.GetTimeline(parseLimit(r)),
	})
}

// ListenAndServe starts the server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s.Router)
}

// --- Metrics helpers ---

// updateNodeGauges refreshes node count gauges from the registry.
func (s *Server) updateNodeGauges() {
	if s.PromMetrics == nil {
		return
	}
	s.PromMetrics.NodeRegistered(s.Registry.OnlineCount(), s.Registry.Count())
}

// updateTaskGauges refreshes task status count gauges from the task store.
func (s *Server) updateTaskGauges() {
	if s.PromMetrics == nil {
		return
	}
	tasks := s.Scheduler.GetTaskStore().GetAll()
	statusCounts := make(map[string]float64)
	for _, t := range tasks {
		statusCounts[string(t.Status)]++
	}
	for status, count := range statusCounts {
		s.PromMetrics.TaskStatusUpdate(status, count)
	}
}

// updateLeaseGauges refreshes active lease count gauge from the lease manager.
func (s *Server) updateLeaseGauges() {
	if s.PromMetrics == nil {
		return
	}
	s.PromMetrics.LeasesActiveUpdate(float64(len(s.LeaseMgr.GetActiveLeases())))
}

// updateSyncGauge refreshes the sync version gauge.
func (s *Server) updateSyncGauge() {
	if s.PromMetrics == nil {
		return
	}
	s.PromMetrics.SyncVersionUpdate(float64(s.StateStore.Version()))
}

// --- Status handler ---

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	filter := status.Filter{
		NodeID:     r.URL.Query().Get("node"),
		Status:     r.URL.Query().Get("status"),
		Capability: r.URL.Query().Get("capability"),
	}
	entries, summary := s.StatusView.Query(filter)
	writeJSON(w, map[string]interface{}{
		"entries": entries,
		"summary": summary,
	})
}
