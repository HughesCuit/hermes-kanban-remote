package visualization

import "time"

// ClusterTimeline holds a list of recent cluster events.
type ClusterTimeline struct {
	Events []TimelineEvent `json:"events"`
}

// TimelineEvent represents a single event in the cluster timeline.
type TimelineEvent struct {
	Type        string    `json:"type"`
	Timestamp   time.Time `json:"timestamp"`
	NodeID      string    `json:"node_id,omitempty"`
	TaskID      string    `json:"task_id,omitempty"`
	Description string    `json:"description"`
}
