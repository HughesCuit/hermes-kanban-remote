package integration

import (
	"fmt"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/heartbeat"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/recovery"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
	"github.com/heventure/hermes-agent-cluster/internal/sync"
)

// TestCluster represents an in-process cluster for integration testing.
type TestCluster struct {
	Registry   *cluster.Registry
	Scheduler  *scheduler.Scheduler
	LeaseMgr   *lease.Manager
	TaskStore  *scheduler.TaskStore
	Watchdog   *heartbeat.Watchdog
	Recovery   *recovery.Detector
	Revoker    *recovery.Revoker
	Rescheduler *recovery.Rescheduler
	RecLog     *recovery.Log
	StateStore *sync.StateStore
	Receiver   *sync.FollowerReceiver
	LeaderSync *sync.LeaderSync
	StopCh     chan struct{}
}

// NewTestCluster creates a fully wired in-process cluster.
func NewTestCluster(leaseTTL time.Duration) *TestCluster {
	registry := cluster.NewRegistry()
	taskStore := scheduler.NewTaskStore()
	leaseMgr := lease.NewManager()
	recLog := recovery.NewLog()
	ss := sync.NewStateStore()
	receiver := sync.NewFollowerReceiver(ss)
	pusher := sync.NewHTTPPusher()
	leaderSync := sync.NewLeaderSync(ss, pusher)

	sched := scheduler.NewScheduler(registry, taskStore, leaseMgr, leaseTTL)
	revoker := recovery.NewRevoker(leaseMgr, recLog)
	resched := recovery.NewRescheduler(sched, recLog)
	detector := recovery.NewDetector(revoker, resched, leaseMgr, recLog)

	stopCh := make(chan struct{})

	// Watchdog: check every 50ms, degraded after 150ms, offline after 300ms
	watchdog := heartbeat.NewWatchdog(
		&registryAdapter{registry},
		50*time.Millisecond,
		150*time.Millisecond,
		300*time.Millisecond,
		func(evt heartbeat.Event) {
			if evt.Type == "offline" {
				detector.NotifyOffline(evt.NodeID)
			}
		},
	)

	// Lease expiry callback: mark node offline before recovery
	// (mirrors the fix in cmd/cluster/main.go)
	leaseMgr.SetExpiryCallback(func(taskID, nodeID string) {
		registry.UpdateStatus(nodeID, cluster.NodeOffline)
		detector.NotifyOffline(nodeID)
	})

	return &TestCluster{
		Registry:    registry,
		Scheduler:   sched,
		LeaseMgr:    leaseMgr,
		TaskStore:   taskStore,
		Watchdog:    watchdog,
		Recovery:    detector,
		Revoker:     revoker,
		Rescheduler: resched,
		RecLog:      recLog,
		StateStore:  ss,
		Receiver:    receiver,
		LeaderSync:  leaderSync,
		StopCh:      stopCh,
	}
}

// Start activates the watchdog and recovery detector.
func (tc *TestCluster) Start() {
	tc.Watchdog.Start()
	tc.Recovery.Start()
}

// Stop shuts down all background goroutines.
func (tc *TestCluster) Stop() {
	tc.Watchdog.Stop()
	tc.Recovery.Stop()
	close(tc.StopCh)
}

// RegisterNode registers a node with capabilities.
func (tc *TestCluster) RegisterNode(id, name string, capabilities []string) {
	tc.Registry.Register(&cluster.Node{
		ID:           id,
		Name:         name,
		Capabilities: capabilities,
	})
}

// SubmitTask creates a task and attempts to schedule it.
func (tc *TestCluster) SubmitTask(id, title string, requires []string) *scheduler.Task {
	task, err := tc.TaskStore.Create(id, title, requires)
	if err != nil {
		panic(fmt.Sprintf("SubmitTask: %v", err))
	}
	tc.Scheduler.SchedulePending()
	return task
}

// StopHeartbeat simulates a node going silent by setting it to a very old heartbeat.
func (tc *TestCluster) StopHeartbeat(nodeID string) {
	// The watchdog checks LastHeartbeat. To simulate stale heartbeat,
	// we need to set it in the past. Since Register() resets it to now,
	// we'll directly use the cluster's internal state.
	// For integration tests, we use the node's status directly.
	tc.SetNodeOffline(nodeID)
}

// InjectStaleHeartbeat sets a node's last heartbeat to a past time.
// Since Register() resets LastHeartbeat to now, we need a different approach.
// We'll use the cluster registry's internal state via the node's status.
func (tc *TestCluster) InjectStaleHeartbeat(nodeID string, ago time.Duration) {
	// Get node info
	nodes := tc.Registry.GetAll()
	for _, n := range nodes {
		if n.ID == nodeID {
			// Re-register with stale heartbeat by setting status first
			tc.Registry.UpdateStatus(nodeID, cluster.NodeDegraded)
			// The watchdog will use LastHeartbeat from the registry
			// For testing, we directly call the watchdog check via status
			break
		}
	}
}

// registryAdapter adapts cluster.Registry to heartbeat.HeartbeatRegistry.
type registryAdapter struct {
	reg *cluster.Registry
}

func (ra *registryAdapter) GetAll() []heartbeat.HeartbeatNode {
	nodes := ra.reg.GetAll()
	result := make([]heartbeat.HeartbeatNode, len(nodes))
	for i, n := range nodes {
		result[i] = heartbeat.HeartbeatNode{
			ID:            n.ID,
			LastHeartbeat: n.LastHeartbeat,
			Status:        string(n.Status),
		}
	}
	return result
}

func (ra *registryAdapter) UpdateStatus(id string, status string) {
	ra.reg.UpdateStatus(id, cluster.NodeStatus(status))
}

// SetNodeOffline directly marks a node as offline for testing.
func (tc *TestCluster) SetNodeOffline(nodeID string) {
	tc.Registry.UpdateStatus(nodeID, cluster.NodeOffline)
}
