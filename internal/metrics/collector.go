// Package metrics provides Prometheus metrics for hermes-agent-cluster.
//
// All metrics use the "hac_" prefix (hermes-agent-cluster).
// Labels are kept minimal to avoid high cardinality.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector holds all Prometheus metric collectors for the cluster.
// It is safe for concurrent use.
type Collector struct {
	// --- Cluster metrics ---
	NodesTotal    prometheus.Gauge       // total registered nodes by status
	NodesOnline   prometheus.Gauge       // online nodes count
	NodeHeartbeat prometheus.CounterVec   // heartbeats received (labels: node_id)

	// --- Task metrics ---
	TasksTotal          prometheus.GaugeVec // tasks by status
	TasksCreatedTotal   prometheus.Counter  // total tasks created
	TasksCompletedTotal prometheus.Counter  // total tasks completed
	TasksFailedTotal    prometheus.Counter  // total tasks failed
	TasksScheduledTotal prometheus.Counter  // total task->node assignments
	TaskDuration        prometheus.Histogram // time from creation to terminal state
	ScheduleDuration    prometheus.Histogram // time from ready to assigned

	// --- Lease metrics ---
	LeasesActive     prometheus.Gauge   // currently active leases
	LeasesCreatedTotal prometheus.Counter
	LeasesExpiredTotal prometheus.Counter
	LeasesRevokedTotal prometheus.Counter
	LeaseDuration     prometheus.Histogram

	// --- Sync metrics ---
	SyncVersion prometheus.Gauge // current state version

	// --- Recovery metrics ---
	RecoveryEventsTotal      *prometheus.CounterVec // labels: action, status
	RecoveryTasksRescheduled prometheus.Counter

	// --- Workflow metrics ---
	WorkflowTriggersTotal   prometheus.Counter // dependency triggers fired
	WorkflowResolutionsTotal prometheus.Counter // successful dependency resolutions

	// --- HTTP API metrics (filled by middleware) ---
	HTTPRequestsTotal    *prometheus.CounterVec  // labels: method, path, status_code
	HTTPRequestDuration  *prometheus.HistogramVec // labels: method, path
}

// New creates and registers a new Collector with the default prometheus registry.
func New() *Collector {
	return NewWithRegistry(prometheus.DefaultRegisterer)
}

// NewWithRegistry creates a new Collector registered with the given prometheus Registerer.
// This allows tests to use isolated registries and avoid global state pollution.
func NewWithRegistry(reg prometheus.Registerer) *Collector {
	factory := promauto.With(reg)

	return &Collector{
		// --- Cluster ---
		NodesTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "hac_nodes_total",
			Help: "Total number of registered cluster nodes by status.",
		}),
		NodesOnline: factory.NewGauge(prometheus.GaugeOpts{
			Name: "hac_nodes_online",
			Help: "Number of currently online cluster nodes.",
		}),
		NodeHeartbeat: *factory.NewCounterVec(prometheus.CounterOpts{
			Name: "hac_node_heartbeats_total",
			Help: "Total heartbeats received from nodes.",
		}, []string{"node_id"}),

		// --- Tasks ---
		TasksTotal: *factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hac_tasks_total",
			Help: "Number of tasks in each status.",
		}, []string{"status"}),
		TasksCreatedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_tasks_created_total",
			Help: "Total tasks created.",
		}),
		TasksCompletedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_tasks_completed_total",
			Help: "Total tasks completed successfully.",
		}),
		TasksFailedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_tasks_failed_total",
			Help: "Total tasks that failed.",
		}),
		TasksScheduledTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_tasks_scheduled_total",
			Help: "Total task-to-node assignments.",
		}),
		TaskDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "hac_task_duration_seconds",
			Help:    "Time from task creation to terminal state (completed/failed).",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		}),
		ScheduleDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "hac_task_schedule_duration_seconds",
			Help:    "Time from task ready to assigned to a node.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		}),

		// --- Leases ---
		LeasesActive: factory.NewGauge(prometheus.GaugeOpts{
			Name: "hac_leases_active",
			Help: "Number of currently active leases.",
		}),
		LeasesCreatedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_leases_created_total",
			Help: "Total leases created.",
		}),
		LeasesExpiredTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_leases_expired_total",
			Help: "Total leases that expired.",
		}),
		LeasesRevokedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_leases_revoked_total",
			Help: "Total leases that were revoked.",
		}),
		LeaseDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "hac_lease_duration_seconds",
			Help:    "Lease TTL duration in seconds.",
			Buckets: []float64{10, 30, 60, 120, 300, 600},
		}),

		// --- Sync ---
		SyncVersion: factory.NewGauge(prometheus.GaugeOpts{
			Name: "hac_sync_version",
			Help: "Current state synchronization version.",
		}),

		// --- Recovery ---
		RecoveryEventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "hac_recovery_events_total",
			Help: "Total recovery events by action and status.",
		}, []string{"action", "status"}),
		RecoveryTasksRescheduled: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_recovery_tasks_rescheduled_total",
			Help: "Total tasks rescheduled after node failure.",
		}),

		// --- Workflow ---
		WorkflowTriggersTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_workflow_triggers_total",
			Help: "Total dependency triggers fired.",
		}),
		WorkflowResolutionsTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "hac_workflow_resolutions_total",
			Help: "Total successful dependency resolutions.",
		}),

		// --- HTTP API ---
		HTTPRequestsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "hac_http_requests_total",
			Help: "Total HTTP requests handled.",
		}, []string{"method", "path", "status_code"}),
		HTTPRequestDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hac_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"method", "path"}),
	}
}

// TaskCreated records a task creation event.
func (c *Collector) TaskCreated() {
	c.TasksCreatedTotal.Inc()
}

// TaskCompleted records a task completion and its duration.
func (c *Collector) TaskCompleted(createdAt time.Time) {
	c.TasksCompletedTotal.Inc()
	c.TaskDuration.Observe(time.Since(createdAt).Seconds())
}

// TaskFailed records a task failure and its duration.
func (c *Collector) TaskFailed(createdAt time.Time) {
	c.TasksFailedTotal.Inc()
	c.TaskDuration.Observe(time.Since(createdAt).Seconds())
}

// TaskScheduled records a task assignment and its scheduling duration.
func (c *Collector) TaskScheduled(readyAt time.Time) {
	c.TasksScheduledTotal.Inc()
	c.ScheduleDuration.Observe(time.Since(readyAt).Seconds())
}

// TaskStatusUpdate updates the task count gauge for a given status.
func (c *Collector) TaskStatusUpdate(status string, count float64) {
	c.TasksTotal.WithLabelValues(status).Set(count)
}

// NodeRegistered records a node registration event.
func (c *Collector) NodeRegistered(onlineCount int, totalCount int) {
	c.NodesOnline.Set(float64(onlineCount))
	c.NodesTotal.Set(float64(totalCount))
}

// NodeHeartbeatReceived records a heartbeat from a node.
func (c *Collector) NodeHeartbeatReceived(nodeID string) {
	c.NodeHeartbeat.WithLabelValues(nodeID).Inc()
}

// LeaseCreated records a lease creation.
func (c *Collector) LeaseCreated(ttl time.Duration) {
	c.LeasesCreatedTotal.Inc()
	c.LeaseDuration.Observe(ttl.Seconds())
}

// LeaseExpired records a lease expiry.
func (c *Collector) LeaseExpired() {
	c.LeasesExpiredTotal.Inc()
}

// LeaseRevoked records a lease revocation.
func (c *Collector) LeaseRevoked() {
	c.LeasesRevokedTotal.Inc()
}

// LeasesActiveUpdate updates the active lease count.
func (c *Collector) LeasesActiveUpdate(count float64) {
	c.LeasesActive.Set(count)
}

// SyncVersionUpdate updates the sync version gauge.
func (c *Collector) SyncVersionUpdate(version float64) {
	c.SyncVersion.Set(version)
}

// RecoveryEvent records a recovery event.
func (c *Collector) RecoveryEvent(action, status string) {
	c.RecoveryEventsTotal.WithLabelValues(action, status).Inc()
}

// RecoveryReschedule records a task reschedule after failure.
func (c *Collector) RecoveryReschedule() {
	c.RecoveryTasksRescheduled.Inc()
}

// WorkflowTrigger records a dependency trigger.
func (c *Collector) WorkflowTrigger() {
	c.WorkflowTriggersTotal.Inc()
}

// WorkflowResolution records a successful dependency resolution.
func (c *Collector) WorkflowResolution() {
	c.WorkflowResolutionsTotal.Inc()
}
