package scheduler

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// TaskStatus represents the state of a task.
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskReady     TaskStatus = "ready"
	TaskAssigned  TaskStatus = "assigned"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskBlocked   TaskStatus = "blocked"
)

// IsTerminal returns true if the task is in a final state.
func IsTerminal(s TaskStatus) bool {
	return s == TaskCompleted || s == TaskFailed
}

// DefaultPriority is the default task priority when none is specified (1=highest, 5=lowest).
const DefaultPriority = 3

// Task represents a schedulable unit of work.
type Task struct {
	ID          string     `json:"id"`
	Title       string     `json:"title"`
	Requires    []string   `json:"requires"`
	DependsOn   []string   `json:"depends_on,omitempty"` // task IDs this task depends on
	Priority    int        `json:"priority"`             // 1=highest, 5=lowest, default 3
	Status      TaskStatus `json:"status"`
	AssignedTo  string     `json:"assigned_to,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	Version     int64      `json:"version"`
	FailReason  string     `json:"fail_reason,omitempty"`
}

// TaskStore is an in-memory store for tasks.
type TaskStore struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

// NewTaskStore creates a new task store.
func NewTaskStore() *TaskStore {
	return &TaskStore{tasks: make(map[string]*Task)}
}

// GenerateID creates a random task ID using crypto/rand (collision-free).
func GenerateID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fallback: should never happen with crypto/rand
		return fmt.Sprintf("task_%d", time.Now().UnixNano())
	}
	return "task_" + hex.EncodeToString(b)
}

// normalizePriority clamps priority to [1,5] and defaults to 3 if zero/negative.
func normalizePriority(p int) int {
	if p < 1 {
		return DefaultPriority
	}
	if p > 5 {
		return 5
	}
	return p
}

// Create adds a new task. Returns an error if the ID already exists.
// Priority defaults to DefaultPriority (3) if set to 0.
func (s *TaskStore) Create(id, title string, requires []string) (*Task, error) {
	return s.CreateWithPriority(id, title, requires, DefaultPriority)
}

// CreateWithPriority adds a new task with an explicit priority (1=highest, 5=lowest).
func (s *TaskStore) CreateWithPriority(id, title string, requires []string, priority int) (*Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tasks[id]; exists {
		return nil, fmt.Errorf("task %s already exists", id)
	}
	now := time.Now()
	t := &Task{
		ID:        id,
		Title:     title,
		Requires:  requires,
		Priority:  normalizePriority(priority),
		Status:    TaskReady,
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}
	s.tasks[id] = t
	return t, nil
}

// Get returns a task by ID.
func (s *TaskStore) Get(id string) (*Task, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tasks[id]
	if !ok {
		return nil, false
	}
	cp := *t
	return &cp, true
}

// Update modifies a task.
func (s *TaskStore) Update(t *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t.UpdatedAt = time.Now()
	t.Version++
	cp := *t
	s.tasks[t.ID] = &cp
}

// SetStatus changes a task's status.
func (s *TaskStore) SetStatus(id string, status TaskStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}
	t.Status = status
	t.UpdatedAt = time.Now()
	t.Version++
	return nil
}

// PromoteIfPending atomically changes a task's status from pending to the
// given status. Returns true only if the task was pending and got promoted.
// This is safe for concurrent callers — only one will succeed.
func (s *TaskStore) PromoteIfPending(id string, to TaskStatus) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok || t.Status != TaskPending {
		return false
	}
	t.Status = to
	t.UpdatedAt = time.Now()
	t.Version++
	return true
}

// SetAssigned marks a task as assigned to a node.
func (s *TaskStore) SetAssigned(id, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}
	t.Status = TaskAssigned
	t.AssignedTo = nodeID
	t.UpdatedAt = time.Now()
	t.Version++
	return nil
}

// Unassign releases a task back to ready.
func (s *TaskStore) Unassign(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}
	t.Status = TaskReady
	t.AssignedTo = ""
	t.UpdatedAt = time.Now()
	t.Version++
	return nil
}

// GetReady returns all tasks in Ready status, sorted by priority ascending
// (lower number = higher priority), then FIFO (earlier CreatedAt first).
func (s *TaskStore) GetReady() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Task
	for _, t := range s.tasks {
		if t.Status == TaskReady {
			cp := *t
			result = append(result, &cp)
		}
	}
	sortTasksByPriorityThenFIFO(result)
	return result
}

// sortTasksByPriorityThenFIFO sorts tasks: priority ascending (1=highest first),
// then by CreatedAt ascending (FIFO within same priority).
func sortTasksByPriorityThenFIFO(tasks []*Task) {
	sort.SliceStable(tasks, func(i, j int) bool {
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority < tasks[j].Priority // lower number = higher priority
		}
		return tasks[i].CreatedAt.Before(tasks[j].CreatedAt) // FIFO
	})
}

// GetAll returns all tasks.
func (s *TaskStore) GetAll() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Task, 0, len(s.tasks))
	for _, t := range s.tasks {
		cp := *t
		result = append(result, &cp)
	}
	return result
}

// GetAllMap returns a copy of all tasks keyed by ID.
func (s *TaskStore) GetAllMap() map[string]*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*Task, len(s.tasks))
	for id, t := range s.tasks {
		cp := *t
		result[id] = &cp
	}
	return result
}

// GetPending returns all tasks in Pending status.
func (s *TaskStore) GetPending() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Task
	for _, t := range s.tasks {
		if t.Status == TaskPending {
			cp := *t
			result = append(result, &cp)
		}
	}
	sortTasksByPriorityThenFIFO(result)
	return result
}

// GetBlocked returns all tasks in Blocked status.
func (s *TaskStore) GetBlocked() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Task
	for _, t := range s.tasks {
		if t.Status == TaskBlocked {
			cp := *t
			result = append(result, &cp)
		}
	}
	return result
}

// SetBlocked marks a task as blocked with a reason.
func (s *TaskStore) SetBlocked(id, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}
	t.Status = TaskBlocked
	t.FailReason = reason
	t.UpdatedAt = time.Now()
	t.Version++
	return nil
}

// Unblock unblocks a task and sets it back to pending.
func (s *TaskStore) Unblock(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}
	if t.Status != TaskBlocked {
		return fmt.Errorf("task %s is not blocked", id)
	}
	t.Status = TaskPending
	t.FailReason = ""
	t.UpdatedAt = time.Now()
	t.Version++
	return nil
}

// CountByStatus returns a map of task counts by status.
func (s *TaskStore) CountByStatus() map[TaskStatus]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := make(map[TaskStatus]int)
	for _, t := range s.tasks {
		counts[t.Status]++
	}
	return counts
}

// GetByNode returns all tasks assigned to a specific node.
func (s *TaskStore) GetByNode(nodeID string) []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Task
	for _, t := range s.tasks {
		if t.AssignedTo == nodeID {
			cp := *t
			result = append(result, &cp)
		}
	}
	return result
}

// ActiveCountForNode returns the number of tasks assigned to a node
// (in assigned or running status).
func (s *TaskStore) ActiveCountForNode(nodeID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, t := range s.tasks {
		if t.AssignedTo == nodeID && (t.Status == TaskAssigned || t.Status == TaskRunning) {
			count++
		}
	}
	return count
}
