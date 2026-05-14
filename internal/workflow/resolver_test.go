package workflow

import (
	"testing"

	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

func newTestStore() *scheduler.TaskStore {
	return scheduler.NewTaskStore()
}

// setDeps sets DependsOn directly on a task without cycle checking (for test setup).
func setDeps(store *scheduler.TaskStore, taskID string, dependsOn []string) {
	t, ok := store.Get(taskID)
	if !ok {
		panic("task not found: " + taskID)
	}
	t.DependsOn = dependsOn
	store.Update(t)
}

func TestDetectCycle_NoCycle(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	setDeps(store, "B", []string{"A"})
	setDeps(store, "C", []string{"B"})

	hasCycle, path := DetectCycle(store)
	if hasCycle {
		t.Fatalf("expected no cycle, got path: %v", path)
	}
}

func TestDetectCycle_DirectCycle(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	setDeps(store, "A", []string{"B"})
	setDeps(store, "B", []string{"A"})

	hasCycle, _ := DetectCycle(store)
	if !hasCycle {
		t.Fatal("expected cycle A→B→A")
	}
}

func TestDetectCycle_IndirectCycle(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	setDeps(store, "A", []string{"C"})
	setDeps(store, "B", []string{"A"})
	setDeps(store, "C", []string{"B"})

	hasCycle, path := DetectCycle(store)
	if !hasCycle {
		t.Fatal("expected cycle A→B→C→A")
	}
	t.Logf("cycle path: %v", path)
}

func TestDetectCycle_NoNodes(t *testing.T) {
	store := newTestStore()
	hasCycle, _ := DetectCycle(store)
	if hasCycle {
		t.Fatal("expected no cycle in empty store")
	}
}

func TestDetectCycle_DiamondNoCycle(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	store.Create("D", "Task D", nil)
	setDeps(store, "B", []string{"A"})
	setDeps(store, "C", []string{"A"})
	setDeps(store, "D", []string{"B", "C"})

	hasCycle, _ := DetectCycle(store)
	if hasCycle {
		t.Fatal("diamond graph should not have cycles")
	}
}

func TestResolveDependencies_AllMet(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	resolver := NewResolver(store)

	// Set B depends on A, which transitions B to PENDING
	resolver.SetDependencies("B", []string{"A"})

	got, _ := store.Get("B")
	if got.Status != scheduler.TaskPending {
		t.Fatalf("expected B to be pending, got %s", got.Status)
	}

	// Complete A
	store.SetStatus("A", scheduler.TaskCompleted)

	// Resolve B's dependencies
	ok, err := resolver.ResolveDependencies("B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected B to transition to ready")
	}

	b, _ := store.Get("B")
	if b.Status != scheduler.TaskReady {
		t.Fatalf("expected B ready, got %s", b.Status)
	}
}

func TestResolveDependencies_NotAllMet(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("C", []string{"A", "B"})

	// Complete only A
	store.SetStatus("A", scheduler.TaskCompleted)

	ok, err := resolver.ResolveDependencies("C")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("C should not be ready yet (B not complete)")
	}

	c, _ := store.Get("C")
	if c.Status != scheduler.TaskPending {
		t.Fatalf("expected C still pending, got %s", c.Status)
	}
}

func TestOnDependencyComplete_TransitionsDownstream(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"A"})

	// Mark A as pending to simulate it running
	store.SetStatus("A", scheduler.TaskRunning)
	store.SetStatus("A", scheduler.TaskCompleted)

	transitioned := resolver.OnDependencyComplete("A")
	if len(transitioned) != 2 {
		t.Fatalf("expected 2 tasks transitioned, got %d: %v", len(transitioned), transitioned)
	}

	b, _ := store.Get("B")
	c, _ := store.Get("C")
	if b.Status != scheduler.TaskReady {
		t.Fatalf("B should be ready, got %s", b.Status)
	}
	if c.Status != scheduler.TaskReady {
		t.Fatalf("C should be ready, got %s", c.Status)
	}
}

func TestIntegration_ABC(t *testing.T) {
	store := newTestStore()
	resolver := NewResolver(store)

	// Create chain A→B→C
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"B"})

	// Verify initial states
	a, _ := store.Get("A")
	b, _ := store.Get("B")
	c, _ := store.Get("C")
	if a.Status != scheduler.TaskReady {
		t.Fatalf("A should be ready, got %s", a.Status)
	}
	if b.Status != scheduler.TaskPending {
		t.Fatalf("B should be pending, got %s", b.Status)
	}
	if c.Status != scheduler.TaskPending {
		t.Fatalf("C should be pending, got %s", c.Status)
	}

	// Complete A → B should become ready
	store.SetStatus("A", scheduler.TaskCompleted)
	transitioned := resolver.OnDependencyComplete("A")
	if len(transitioned) != 1 || transitioned[0] != "B" {
		t.Fatalf("expected [B] transitioned, got %v", transitioned)
	}

	b, _ = store.Get("B")
	c, _ = store.Get("C")
	if b.Status != scheduler.TaskReady {
		t.Fatalf("B should be ready after A completes, got %s", b.Status)
	}
	if c.Status != scheduler.TaskPending {
		t.Fatalf("C should still be pending, got %s", c.Status)
	}

	// Complete B → C should become ready
	store.SetStatus("B", scheduler.TaskCompleted)
	transitioned = resolver.OnDependencyComplete("B")
	if len(transitioned) != 1 || transitioned[0] != "C" {
		t.Fatalf("expected [C] transitioned, got %v", transitioned)
	}

	c, _ = store.Get("C")
	if c.Status != scheduler.TaskReady {
		t.Fatalf("C should be ready after B completes, got %s", c.Status)
	}
}

func TestSetDependencies_RejectsCycle(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})

	// Try to make A depend on B → cycle A→B→A
	err := resolver.SetDependencies("A", []string{"B"})
	if err == nil {
		t.Fatal("expected cycle error")
	}

	// Verify A was rolled back to no dependencies
	a, _ := store.Get("A")
	if len(a.DependsOn) != 0 {
		t.Fatalf("A's DependsOn should be rolled back, got %v", a.DependsOn)
	}
}

func TestGetDependents(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	store.Create("D", "Task D", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"A"})

	deps := resolver.GetDependents("A")
	if len(deps) != 2 {
		t.Fatalf("expected 2 dependents for A, got %d: %v", len(deps), deps)
	}

	deps = resolver.GetDependents("D")
	if len(deps) != 0 {
		t.Fatalf("expected 0 dependents for D, got %d", len(deps))
	}
}

func TestGetGraph(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})

	graph := resolver.GetGraph()

	if len(graph.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(graph.Edges))
	}
	if graph.Edges[0].From != "A" || graph.Edges[0].To != "B" {
		t.Fatalf("expected edge A→B, got %s→%s", graph.Edges[0].From, graph.Edges[0].To)
	}
}

// --- New tests for Trigger Mechanism ---

func TestOnDependencyFailed_BlocksDownstream(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"A"})

	// Fail A
	store.SetStatus("A", scheduler.TaskFailed)

	// Block downstream tasks
	blocked := resolver.OnDependencyFailed("A")
	if len(blocked) != 2 {
		t.Fatalf("expected 2 tasks blocked, got %d: %v", len(blocked), blocked)
	}

	b, _ := store.Get("B")
	c, _ := store.Get("C")
	if b.Status != scheduler.TaskBlocked {
		t.Fatalf("B should be blocked, got %s", b.Status)
	}
	if c.Status != scheduler.TaskBlocked {
		t.Fatalf("C should be blocked, got %s", c.Status)
	}
}

func TestOnDependencyFailed_PartialBlock(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	store.Create("D", "Task D", nil)
	resolver := NewResolver(store)
	// C depends on both A and B
	resolver.SetDependencies("C", []string{"A", "B"})
	// D depends only on A
	resolver.SetDependencies("D", []string{"A"})

	// Fail A
	store.SetStatus("A", scheduler.TaskFailed)

	// Block downstream tasks
	blocked := resolver.OnDependencyFailed("A")
	if len(blocked) != 2 {
		t.Fatalf("expected 2 tasks blocked, got %d: %v", len(blocked), blocked)
	}

	c, _ := store.Get("C")
	d, _ := store.Get("D")
	if c.Status != scheduler.TaskBlocked {
		t.Fatalf("C should be blocked, got %s", c.Status)
	}
	if d.Status != scheduler.TaskBlocked {
		t.Fatalf("D should be blocked, got %s", d.Status)
	}
}

func TestOnDependencyFailed_NoDownstream(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	resolver := NewResolver(store)
	// B depends on A
	resolver.SetDependencies("B", []string{"A"})

	// Fail B (no downstream)
	store.SetStatus("B", scheduler.TaskFailed)

	blocked := resolver.OnDependencyFailed("B")
	if len(blocked) != 0 {
		t.Fatalf("expected 0 tasks blocked, got %d: %v", len(blocked), blocked)
	}
}

func TestManualAdvance_FromPending(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})

	// B should be pending
	b, _ := store.Get("B")
	if b.Status != scheduler.TaskPending {
		t.Fatalf("B should be pending, got %s", b.Status)
	}

	// Manually advance B
	err := resolver.ManualAdvance("B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, _ = store.Get("B")
	if b.Status != scheduler.TaskReady {
		t.Fatalf("B should be ready after manual advance, got %s", b.Status)
	}
}

func TestManualAdvance_FromBlocked(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})

	// Fail A to block B
	store.SetStatus("A", scheduler.TaskFailed)
	resolver.OnDependencyFailed("A")

	b, _ := store.Get("B")
	if b.Status != scheduler.TaskBlocked {
		t.Fatalf("B should be blocked, got %s", b.Status)
	}

	// Manually advance B
	err := resolver.ManualAdvance("B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, _ = store.Get("B")
	if b.Status != scheduler.TaskReady {
		t.Fatalf("B should be ready after manual advance, got %s", b.Status)
	}
}

func TestManualAdvance_AlreadyReady(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	resolver := NewResolver(store)

	// A is already ready
	err := resolver.ManualAdvance("A")
	if err == nil {
		t.Fatal("expected error for already ready task")
	}
}

func TestManualAdvance_RunningTask(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.SetStatus("A", scheduler.TaskRunning)
	resolver := NewResolver(store)

	err := resolver.ManualAdvance("A")
	if err == nil {
		t.Fatal("expected error for running task")
	}
}

func TestGetTriggerChain(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	store.Create("D", "Task D", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"A"})
	resolver.SetDependencies("D", []string{"B"})

	chain := resolver.GetTriggerChain("A")
	if len(chain) != 3 {
		t.Fatalf("expected 3 tasks in chain, got %d: %v", len(chain), chain)
	}
	// A triggers B and C, B triggers D
	// So chain should contain B, C, D
	found := make(map[string]bool)
	for _, id := range chain {
		found[id] = true
	}
	if !found["B"] || !found["C"] || !found["D"] {
		t.Fatalf("expected B, C, D in chain, got %v", chain)
	}
}

func TestGetTriggerChain_NoDownstream(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	resolver := NewResolver(store)

	chain := resolver.GetTriggerChain("A")
	if len(chain) != 0 {
		t.Fatalf("expected empty chain, got %d: %v", len(chain), chain)
	}
}

func TestTriggerChainDepthLimit(t *testing.T) {
	store := newTestStore()
	resolver := NewResolver(store)

	// Create a chain: A→B→C→D→...→K (11 tasks)
	store.Create("A", "Task A", nil)
	for i := 1; i <= 10; i++ {
		id := string(rune('A' + i))
		store.Create(id, "Task "+id, nil)
		resolver.SetDependencies(id, []string{string(rune('A' + i - 1))})
	}

	// Complete A → should trigger up to depth limit
	store.SetStatus("A", scheduler.TaskCompleted)
	transitioned := resolver.OnDependencyComplete("A")

	// Should have transitioned at most MaxTriggerChainDepth tasks
	if len(transitioned) > MaxTriggerChainDepth {
		t.Fatalf("expected at most %d tasks transitioned, got %d", MaxTriggerChainDepth, len(transitioned))
	}
}

func TestIntegration_DiamondWorkflow(t *testing.T) {
	store := newTestStore()
	resolver := NewResolver(store)

	// Create diamond: A→B, A→C, B→D, C→D
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	store.Create("D", "Task D", nil)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"A"})
	resolver.SetDependencies("D", []string{"B", "C"})

	// Verify initial states
	a, _ := store.Get("A")
	b, _ := store.Get("B")
	c, _ := store.Get("C")
	d, _ := store.Get("D")
	if a.Status != scheduler.TaskReady {
		t.Fatalf("A should be ready, got %s", a.Status)
	}
	if b.Status != scheduler.TaskPending {
		t.Fatalf("B should be pending, got %s", b.Status)
	}
	if c.Status != scheduler.TaskPending {
		t.Fatalf("C should be pending, got %s", c.Status)
	}
	if d.Status != scheduler.TaskPending {
		t.Fatalf("D should be pending, got %s", d.Status)
	}

	// Complete A → B and C should become ready
	store.SetStatus("A", scheduler.TaskCompleted)
	transitioned := resolver.OnDependencyComplete("A")
	if len(transitioned) != 2 {
		t.Fatalf("expected 2 tasks transitioned, got %d: %v", len(transitioned), transitioned)
	}

	b, _ = store.Get("B")
	c, _ = store.Get("C")
	if b.Status != scheduler.TaskReady {
		t.Fatalf("B should be ready, got %s", b.Status)
	}
	if c.Status != scheduler.TaskReady {
		t.Fatalf("C should be ready, got %s", c.Status)
	}

	// Complete B → D should NOT be ready (C not complete)
	store.SetStatus("B", scheduler.TaskCompleted)
	transitioned = resolver.OnDependencyComplete("B")
	if len(transitioned) != 0 {
		t.Fatalf("expected 0 tasks transitioned, got %d: %v", len(transitioned), transitioned)
	}

	d, _ = store.Get("D")
	if d.Status != scheduler.TaskPending {
		t.Fatalf("D should still be pending, got %s", d.Status)
	}

	// Complete C → D should become ready
	store.SetStatus("C", scheduler.TaskCompleted)
	transitioned = resolver.OnDependencyComplete("C")
	if len(transitioned) != 1 || transitioned[0] != "D" {
		t.Fatalf("expected [D] transitioned, got %v", transitioned)
	}

	d, _ = store.Get("D")
	if d.Status != scheduler.TaskReady {
		t.Fatalf("D should be ready, got %s", d.Status)
	}
}

func TestIntegration_FailurePropagation(t *testing.T) {
	store := newTestStore()
	resolver := NewResolver(store)

	// Create chain: A→B→C
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"B"})

	// Complete A
	store.SetStatus("A", scheduler.TaskCompleted)
	resolver.OnDependencyComplete("A")

	// Fail B
	store.SetStatus("B", scheduler.TaskFailed)
	blocked := resolver.OnDependencyFailed("B")
	if len(blocked) != 1 || blocked[0] != "C" {
		t.Fatalf("expected [C] blocked, got %v", blocked)
	}

	c, _ := store.Get("C")
	if c.Status != scheduler.TaskBlocked {
		t.Fatalf("C should be blocked, got %s", c.Status)
	}

	// Manually advance C despite failure
	err := resolver.ManualAdvance("C")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	c, _ = store.Get("C")
	if c.Status != scheduler.TaskReady {
		t.Fatalf("C should be ready after manual advance, got %s", c.Status)
	}
}

func TestIntegration_BlockedRecovery(t *testing.T) {
	store := newTestStore()
	resolver := NewResolver(store)

	// Create: A→B, A→C
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"A"})

	// Fail A → B and C blocked
	store.SetStatus("A", scheduler.TaskFailed)
	blocked := resolver.OnDependencyFailed("A")
	if len(blocked) != 2 {
		t.Fatalf("expected 2 tasks blocked, got %d: %v", len(blocked), blocked)
	}

	// Unblock B manually
	err := store.Unblock("B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, _ := store.Get("B")
	if b.Status != scheduler.TaskPending {
		t.Fatalf("B should be pending after unblock, got %s", b.Status)
	}

	// Resolve B's dependencies (still fails because A is failed)
	ok, _ := resolver.ResolveDependencies("B")
	if ok {
		t.Fatal("B should not be ready (A still failed)")
	}

	b, _ = store.Get("B")
	if b.Status != scheduler.TaskBlocked {
		t.Fatalf("B should be blocked again, got %s", b.Status)
	}
}

func TestFormatTriggerChain(t *testing.T) {
	store := newTestStore()
	store.Create("A", "Task A", nil)
	store.Create("B", "Task B", nil)
	store.Create("C", "Task C", nil)
	resolver := NewResolver(store)
	resolver.SetDependencies("B", []string{"A"})
	resolver.SetDependencies("C", []string{"B"})

	result := resolver.FormatTriggerChain("A")
	if result != "A → B → C" {
		t.Fatalf("expected 'A → B → C', got '%s'", result)
	}

	result = resolver.FormatTriggerChain("C")
	if result != "C (no downstream tasks)" {
		t.Fatalf("expected 'C (no downstream tasks)', got '%s'", result)
	}
}
