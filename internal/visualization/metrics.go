package visualization

// ClusterMetrics holds aggregate cluster metrics.
type ClusterMetrics struct {
	Nodes    []NodeMetric  `json:"nodes"`
	Tasks    TaskMetric    `json:"tasks"`
	Leases   LeaseMetric   `json:"leases"`
}

// NodeMetric holds per-node utilization metrics.
type NodeMetric struct {
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	Status        string  `json:"status"`
	TasksAssigned int     `json:"tasks_assigned"`
	Load          float64 `json:"load"`
}

// TaskMetric holds task throughput metrics.
type TaskMetric struct {
	Total          int            `json:"total"`
	ByStatus       map[string]int `json:"by_status"`
	CompletionRate float64        `json:"completion_rate"`
}

// LeaseMetric holds lease statistics.
type LeaseMetric struct {
	ActiveCount  int `json:"active_count"`
	ExpiredCount int `json:"expired_count"`
}
