package workflow

import (
	"fmt"
	"strings"

	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

const (
	// MaxTriggerChainDepth limits how many levels deep a trigger chain can propagate.
	// This prevents infinite recursion in complex dependency graphs.
	MaxTriggerChainDepth = 10
)

// Resolver handles dependency resolution for tasks.
type Resolver struct {
	store *scheduler.TaskStore
}

// NewResolver creates a new workflow resolver.
func NewResolver(store *scheduler.TaskStore) *Resolver {
	return &Resolver{store: store}
}

// ResolveDependencies checks if all dependencies of a task are met.
// If yes, transitions the task from PENDING to READY.
// If any dependency failed, transitions to BLOCKED.
func (r *Resolver) ResolveDependencies(taskID string) (bool, error) {
	task, ok := r.store.Get(taskID)
	if !ok {
		return false, fmt.Errorf("task %s not found", taskID)
	}

	if task.Status != scheduler.TaskPending {
		return false, nil
	}

	allDone := true
	anyFailed := false
	for _, depID := range task.DependsOn {
		dep, ok := r.store.Get(depID)
		if !ok {
			allDone = false
			continue
		}
		if dep.Status == scheduler.TaskFailed {
			anyFailed = true
			break
		}
		if !scheduler.IsTerminal(dep.Status) {
			allDone = false
		}
	}

	if anyFailed {
		// Block the task because a dependency failed
		if err := r.store.SetBlocked(taskID, "dependency failed"); err != nil {
			return false, fmt.Errorf("block %s: %w", taskID, err)
		}
		return false, nil
	}

	if allDone {
		if err := r.store.SetStatus(taskID, scheduler.TaskReady); err != nil {
			return false, fmt.Errorf("transition %s to ready: %w", taskID, err)
		}
		return true, nil
	}
	return false, nil
}

// OnDependencyComplete is called when a task completes or fails.
// It scans all pending/blocked tasks and transitions those whose dependencies are now met.
// Returns the list of task IDs that were transitioned.
func (r *Resolver) OnDependencyComplete(completedTaskID string) []string {
	return r.propagate(completedTaskID, 0)
}

// propagate handles the actual propagation with depth tracking.
func (r *Resolver) propagate(completedTaskID string, depth int) []string {
	if depth >= MaxTriggerChainDepth {
		return nil
	}

	var transitioned []string

	// Scan pending tasks
	pending := r.store.GetPending()
	for _, t := range pending {
		for _, depID := range t.DependsOn {
			if depID == completedTaskID {
				if ok, _ := r.ResolveDependencies(t.ID); ok {
					transitioned = append(transitioned, t.ID)
				}
				break
			}
		}
	}

	// Scan blocked tasks (they might become unblocked if we just transitioned)
	blocked := r.store.GetBlocked()
	for _, t := range blocked {
		// Only consider tasks blocked due to dependency failures
		if t.FailReason != "dependency failed" {
			continue
		}
		for _, depID := range t.DependsOn {
			if depID == completedTaskID {
				// Re-evaluate: check if all deps are now terminal and none failed
				allDone := true
				anyFailed := false
				for _, depID2 := range t.DependsOn {
					dep, ok := r.store.Get(depID2)
					if !ok {
						allDone = false
						continue
					}
					if dep.Status == scheduler.TaskFailed {
						anyFailed = true
						break
					}
					if !scheduler.IsTerminal(dep.Status) {
						allDone = false
					}
				}
				if !anyFailed && allDone {
					if err := r.store.SetStatus(t.ID, scheduler.TaskReady); err == nil {
						transitioned = append(transitioned, t.ID)
					}
				}
				break
			}
		}
	}

	// Cascade: if we transitioned tasks, check if their dependents are now ready
	for _, tid := range transitioned {
		inner := r.propagate(tid, depth+1)
		transitioned = append(transitioned, inner...)
	}

	return transitioned
}

// OnDependencyFailed is called when a task fails.
// It blocks all downstream tasks that depend on the failed task.
// Returns the list of task IDs that were blocked.
func (r *Resolver) OnDependencyFailed(failedTaskID string) []string {
	var blocked []string
	pending := r.store.GetPending()
	for _, t := range pending {
		for _, depID := range t.DependsOn {
			if depID == failedTaskID {
				if err := r.store.SetBlocked(t.ID, fmt.Sprintf("dependency %s failed", failedTaskID)); err == nil {
					blocked = append(blocked, t.ID)
				}
				break
			}
		}
	}
	return blocked
}

// ManualAdvance forces a task from PENDING/BLOCKED to READY, ignoring dependencies.
// This is the "hermes remote workflow advance --force" command.
func (r *Resolver) ManualAdvance(taskID string) error {
	task, ok := r.store.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	switch task.Status {
	case scheduler.TaskPending, scheduler.TaskBlocked:
		if err := r.store.SetStatus(taskID, scheduler.TaskReady); err != nil {
			return fmt.Errorf("advance %s: %w", taskID, err)
		}
		return nil
	case scheduler.TaskReady:
		return fmt.Errorf("task %s is already ready", taskID)
	default:
		return fmt.Errorf("task %s cannot be advanced from status %s", taskID, task.Status)
	}
}

// DependencyGraph represents the full dependency graph.
type DependencyGraph struct {
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
}

type GraphNode struct {
	ID     string               `json:"id"`
	Title  string               `json:"title"`
	Status scheduler.TaskStatus `json:"status"`
}

type GraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// GetGraph returns the full dependency graph.
func (r *Resolver) GetGraph() DependencyGraph {
	all := r.store.GetAllMap()
	g := DependencyGraph{}
	for _, t := range all {
		g.Nodes = append(g.Nodes, GraphNode{
			ID:     t.ID,
			Title:  t.Title,
			Status: t.Status,
		})
		for _, depID := range t.DependsOn {
			g.Edges = append(g.Edges, GraphEdge{
				From: depID,
				To:   t.ID,
			})
		}
	}
	return g
}

// GetDependents returns all task IDs that depend on the given task.
func (r *Resolver) GetDependents(taskID string) []string {
	var dependents []string
	all := r.store.GetAllMap()
	for _, t := range all {
		for _, depID := range t.DependsOn {
			if depID == taskID {
				dependents = append(dependents, t.ID)
				break
			}
		}
	}
	return dependents
}

// DetectCycle checks for circular dependencies in the task graph.
// Returns true if a cycle exists, along with the cycle path.
func DetectCycle(store *scheduler.TaskStore) (bool, []string) {
	all := store.GetAllMap()
	const (
		white = 0 // unvisited
		gray  = 1 // in current DFS path
		black = 2 // fully explored
	)

	color := make(map[string]int)
	parent := make(map[string]string)

	for id := range all {
		color[id] = white
	}

	var dfs func(id string) (bool, []string)
	dfs = func(id string) (bool, []string) {
		if color[id] == black {
			return false, nil
		}
		if color[id] == gray {
			// Found cycle - reconstruct path
			cycle := []string{id}
			cur := parent[id]
			for cur != id {
				cycle = append([]string{cur}, cycle...)
				cur = parent[cur]
			}
			cycle = append([]string{id}, cycle...)
			return true, cycle
		}

		color[id] = gray
		task := all[id]
		for _, depID := range task.DependsOn {
			if _, exists := all[depID]; !exists {
				continue // dangling reference, skip
			}
			parent[depID] = id
			if found, path := dfs(depID); found {
				return true, path
			}
		}
		color[id] = black
		return false, nil
	}

	for id := range all {
		if color[id] == white {
			if found, path := dfs(id); found {
				return true, path
			}
		}
	}
	return false, nil
}

// SetDependencies sets the DependsOn list for a task, with cycle checking.
func (r *Resolver) SetDependencies(taskID string, dependsOn []string) error {
	task, ok := r.store.Get(taskID)
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Temporarily set to check for cycles
	task.DependsOn = dependsOn
	r.store.Update(task)

	if hasCycle, cyclePath := DetectCycle(r.store); hasCycle {
		// Roll back
		task.DependsOn = nil
		r.store.Update(task)
		return fmt.Errorf("circular dependency detected: %v", cyclePath)
	}

	// Re-set status based on new dependencies
	if len(dependsOn) > 0 {
		// Check if all deps are already complete
		if ok, _ := r.ResolveDependencies(taskID); !ok {
			// Some deps not met, keep pending
			r.store.SetStatus(taskID, scheduler.TaskPending)
		}
	} else {
		// No deps, transition to ready
		r.store.SetStatus(taskID, scheduler.TaskReady)
	}
	return nil
}

// GetTriggerChain returns the trigger chain for a given task.
// It shows the path of tasks that would be triggered if the given task completes.
func (r *Resolver) GetTriggerChain(taskID string) []string {
	dependents := r.GetDependents(taskID)
	var chain []string
	visited := make(map[string]bool)
	r.walkTriggerChain(dependents, &chain, visited)
	return chain
}

func (r *Resolver) walkTriggerChain(tasks []string, chain *[]string, visited map[string]bool) {
	for _, taskID := range tasks {
		if visited[taskID] || len(*chain) >= MaxTriggerChainDepth {
			continue
		}
		visited[taskID] = true
		*chain = append(*chain, taskID)
		nextDependents := r.GetDependents(taskID)
		r.walkTriggerChain(nextDependents, chain, visited)
	}
}

// FormatTriggerChain returns a human-readable trigger chain for display.
func (r *Resolver) FormatTriggerChain(taskID string) string {
	chain := r.GetTriggerChain(taskID)
	if len(chain) == 0 {
		return taskID + " (no downstream tasks)"
	}
	return taskID + " → " + strings.Join(chain, " → ")
}
