package sync

// EventType represents the type of sync event.
type EventType string

const (
	EventTaskCreated   EventType = "task_created"
	EventTaskAssigned  EventType = "task_assigned"
	EventTaskCompleted EventType = "task_completed"
	EventTaskFailed    EventType = "task_failed"
)

// SyncMessage is the wire format for state synchronization.
type SyncMessage struct {
	Version    int64     `json:"version"`
	SenderNode string    `json:"sender_node"`
	TaskState  *TaskSync `json:"task_state,omitempty"`
	EventType  EventType `json:"event_type"`
	Timestamp  int64     `json:"timestamp"`
}

// TaskSync is the task state carried in sync messages.
type TaskSync struct {
	TaskID     string `json:"task_id"`
	Title      string `json:"title"`
	Status     string `json:"status"`
	AssignedTo string `json:"assigned_to,omitempty"`
	Version    int64  `json:"version"`
}

// BatchSyncMessage wraps multiple SyncMessages for efficient batch transfer.
type BatchSyncMessage struct {
	Messages []SyncMessage `json:"messages"`
}
