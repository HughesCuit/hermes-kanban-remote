package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds all predefined metrics for the cluster.
type Metrics struct {
	// Task lifecycle
	TasksCreated   metric.Int64Counter
	TasksCompleted metric.Int64Counter
	TasksFailed    metric.Int64Counter
	TasksScheduled metric.Int64Counter

	// Lease lifecycle
	LeasesCreated metric.Int64Counter
	LeasesExpired metric.Int64Counter
	LeasesRevoked metric.Int64Counter

	// Node lifecycle
	NodesOnline metric.Int64UpDownCounter

	// Recovery
	RecoveryEvents metric.Int64Counter

	// Sync
	SyncMessagesReceived metric.Int64Counter

	// Scheduling latency
	SchedulingDuration metric.Float64Histogram

	// HTTP request duration
	HTTPRequestDuration metric.Float64Histogram
}

// NewMetrics creates and registers all metrics. Returns a noop Metrics if meter is nil.
func NewMetrics() (*Metrics, error) {
	meter := otel.Meter("hermes-agent-cluster")

	m := &Metrics{}

	var err error

	m.TasksCreated, err = meter.Int64Counter("hac.tasks.created",
		metric.WithDescription("Total tasks created"))
	if err != nil {
		return nil, err
	}

	m.TasksCompleted, err = meter.Int64Counter("hac.tasks.completed",
		metric.WithDescription("Total tasks completed"))
	if err != nil {
		return nil, err
	}

	m.TasksFailed, err = meter.Int64Counter("hac.tasks.failed",
		metric.WithDescription("Total tasks failed"))
	if err != nil {
		return nil, err
	}

	m.TasksScheduled, err = meter.Int64Counter("hac.tasks.scheduled",
		metric.WithDescription("Total tasks scheduled to nodes"))
	if err != nil {
		return nil, err
	}

	m.LeasesCreated, err = meter.Int64Counter("hac.leases.created",
		metric.WithDescription("Total leases created"))
	if err != nil {
		return nil, err
	}

	m.LeasesExpired, err = meter.Int64Counter("hac.leases.expired",
		metric.WithDescription("Total leases expired"))
	if err != nil {
		return nil, err
	}

	m.LeasesRevoked, err = meter.Int64Counter("hac.leases.revoked",
		metric.WithDescription("Total leases revoked"))
	if err != nil {
		return nil, err
	}

	m.NodesOnline, err = meter.Int64UpDownCounter("hac.nodes.online",
		metric.WithDescription("Number of online nodes"))
	if err != nil {
		return nil, err
	}

	m.RecoveryEvents, err = meter.Int64Counter("hac.recovery.events",
		metric.WithDescription("Total recovery events triggered"))
	if err != nil {
		return nil, err
	}

	m.SyncMessagesReceived, err = meter.Int64Counter("hac.sync.messages.received",
		metric.WithDescription("Total sync messages received"))
	if err != nil {
		return nil, err
	}

	m.SchedulingDuration, err = meter.Float64Histogram("hac.scheduler.duration",
		metric.WithDescription("Scheduling operation duration in seconds"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	m.HTTPRequestDuration, err = meter.Float64Histogram("hac.http.request.duration",
		metric.WithDescription("HTTP request duration in seconds"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	return m, nil
}