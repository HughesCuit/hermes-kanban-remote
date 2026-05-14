package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/api"
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/config"
	"github.com/heventure/hermes-agent-cluster/internal/heartbeat"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/recovery"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
	"github.com/heventure/hermes-agent-cluster/internal/sync"
	"github.com/heventure/hermes-agent-cluster/internal/workflow"
)

func main() {
	// --- CLI flags ---
	configPath := flag.String("config", "cluster.yaml", "path to cluster.yaml config file")
	flag.Parse()

	// --- Load config ---
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	log.Printf("starting hermes-agent-cluster | cluster=%s role=%s node=%s",
		cfg.Cluster.ID, cfg.Cluster.Role, cfg.Node.ID)

	// --- Initialize core components ---
	registry := cluster.NewRegistry()
	taskStore := scheduler.NewTaskStore()
	leaseMgr := lease.NewManager()
	recLog := recovery.NewLog()
	stateStore := sync.NewStateStore()
	receiver := sync.NewFollowerReceiver(stateStore)
	pusher := sync.NewHTTPPusher()
	leaderSync := sync.NewLeaderSync(stateStore, pusher)

	// --- Register self ---
	selfNode := &cluster.Node{
		ID:           cfg.Node.ID,
		Name:         cfg.Node.Name,
		Capabilities: cfg.Node.Capabilities,
	}
	registry.Register(selfNode)
	log.Printf("registered node: %s capabilities=%v", cfg.Node.ID, cfg.Node.Capabilities)

	// --- Scheduler: API -> Scheduler -> Lease ---
	sched := scheduler.NewScheduler(registry, taskStore, leaseMgr, cfg.Lease.TTL)

	// --- Workflow Resolver: dependency resolution + trigger mechanism ---
	resolver := workflow.NewResolver(taskStore)

	// --- Trigger: when a node comes online, promote pending tasks and schedule ---
	registry.SetOnNodeOnline(func(nodeID string) {
		promoted := sched.TriggerPendingTasks()
		if len(promoted) > 0 {
			log.Printf("trigger: promoted %d pending tasks for node %s", len(promoted), nodeID)
			sched.SchedulePending()
		}
	})

	// --- Recovery: Detector -> Revoker -> Rescheduler ---
	revoker := recovery.NewRevoker(leaseMgr, recLog)
	rescheduler := recovery.NewRescheduler(sched, recLog)
	detector := recovery.NewDetector(revoker, rescheduler, leaseMgr, recLog)

	// --- Registry adapter for heartbeat watchdog ---
	adapter := &cluster.RegistryAdapter{Reg: registry}

	// --- Watchdog: heartbeat -> offline -> recovery ---
	watchdog := heartbeat.NewWatchdog(
		adapter,
		cfg.Watchdog.CheckInterval,
		cfg.Watchdog.DegradedAfter,
		cfg.Watchdog.OfflineAfter,
		func(evt heartbeat.Event) {
			log.Printf("heartbeat event: node=%s status=%s", evt.NodeID, evt.Type)
			if evt.Type == "offline" {
				detector.NotifyOffline(evt.NodeID)
			}
		},
	)

	// --- Lease expiry callback: triggers recovery on lease expiry ---
	leaseMgr.SetExpiryCallback(func(taskID, nodeID string) {
		log.Printf("lease expired: task=%s node=%s", taskID, nodeID)
		// Mark node as offline in registry so scheduler won't pick it again
		registry.UpdateStatus(nodeID, cluster.NodeOffline)
		detector.NotifyOffline(nodeID)
	})

	// --- Build API server ---
	server := api.NewServer(
		registry,
		sched,
		leaseMgr,
		detector,
		recLog,
		stateStore,
		receiver,
		leaderSync,
		resolver,
	)

	// --- Start background services ---
	stopCh := make(chan struct{})

	// Start lease expiry scanner
	leaseMgr.StartExpiryScanner(cfg.Lease.ScanRate, stopCh)

	// Start watchdog
	watchdog.Start()

	// Start recovery detector
	detector.Start()

	// If this is a leader node, start sync push loop
	if cfg.Cluster.Role == "main" {
		go syncPushLoop(leaderSync, taskStore, cfg.Node.ID, stopCh)
	}

	log.Printf("all subsystems started")

	// --- HTTP server with graceful shutdown ---
	addr := cfg.Server.BindAddress()
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      server.Router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("API listening on %s", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// --- Wait for shutdown signal ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("received signal %v, shutting down...", sig)

	// --- Graceful shutdown ---
	close(stopCh)
	watchdog.Stop()
	detector.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	log.Println("server stopped")
}

// --- Sync Push Loop
// Periodically pushes task state to followers (main node only).

func syncPushLoop(ls *sync.LeaderSync, store *scheduler.TaskStore, senderNode string, stopCh <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastVersion int64

	for {
		select {
		case <-ticker.C:
			// Push any tasks with version > lastVersion
			tasks := store.GetAll()
			for _, t := range tasks {
				if t.Version > lastVersion {
					ls.PushTaskState(sync.TaskSync{
						TaskID:     t.ID,
						Title:      t.Title,
						Status:     string(t.Status),
						AssignedTo: t.AssignedTo,
						Version:    t.Version,
					}, sync.EventTaskCreated, senderNode)
				}
			}
			// Update last seen version with explicit max scan
			var maxVersion int64
			for _, t := range tasks {
				if t.Version > maxVersion {
					maxVersion = t.Version
				}
			}
			if maxVersion > lastVersion {
				lastVersion = maxVersion
			}
		case <-stopCh:
			return
		}
	}
}
