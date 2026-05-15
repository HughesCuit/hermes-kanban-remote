package sync

import (
	"sync"
)

// StateStore manages synchronized task state with version tracking.
type StateStore struct {
	mu      sync.RWMutex
	tasks   map[string]*TaskSync // task_id -> synced state
	version int64                // global version counter
}

// NewStateStore creates a new sync state store.
func NewStateStore() *StateStore {
	return &StateStore{
		tasks: make(map[string]*TaskSync),
	}
}

// Apply applies a sync message using last-write-wins (higher version wins).
func (s *StateStore) Apply(msg SyncMessage) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if msg.TaskState == nil {
		return false
	}

	ts := msg.TaskState
	existing, ok := s.tasks[ts.TaskID]
	if ok && existing.Version >= ts.Version {
		return false // stale message
	}

	s.tasks[ts.TaskID] = &TaskSync{
		TaskID:     ts.TaskID,
		Title:      ts.Title,
		Status:     ts.Status,
		AssignedTo: ts.AssignedTo,
		Version:    ts.Version,
	}
	if ts.Version > s.version {
		s.version = ts.Version
	}
	return true
}

// Get returns the synced state for a task.
func (s *StateStore) Get(taskID string) (*TaskSync, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts, ok := s.tasks[taskID]
	if !ok {
		return nil, false
	}
	cp := *ts
	return &cp, true
}

// GetAll returns all synced tasks.
func (s *StateStore) GetAll() map[string]*TaskSync {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*TaskSync, len(s.tasks))
	for k, v := range s.tasks {
		cp := *v
		result[k] = &cp
	}
	return result
}

// Version returns the current global version.
func (s *StateStore) Version() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// ApplyBatch applies multiple sync messages atomically.
// Returns the number of messages that were actually applied (non-stale).
func (s *StateStore) ApplyBatch(msgs []SyncMessage) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	applied := 0
	for _, msg := range msgs {
		if msg.TaskState == nil {
			continue
		}
		ts := msg.TaskState
		existing, ok := s.tasks[ts.TaskID]
		if ok && existing.Version >= ts.Version {
			continue // stale
		}
		s.tasks[ts.TaskID] = &TaskSync{
			TaskID:     ts.TaskID,
			Title:      ts.Title,
			Status:     ts.Status,
			AssignedTo: ts.AssignedTo,
			Version:    ts.Version,
		}
		if ts.Version > s.version {
			s.version = ts.Version
		}
		applied++
	}
	return applied
}
