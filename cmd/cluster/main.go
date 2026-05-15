package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/api"
	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/config"
	"github.com/heventure/hermes-agent-cluster/internal/federation"
	"github.com/heventure/hermes-agent-cluster/internal/heartbeat"
	"github.com/heventure/hermes-agent-cluster/internal/hooks"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/metrics"
	"github.com/heventure/hermes-agent-cluster/internal/recovery"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
	"github.com/heventure/hermes-agent-cluster/internal/sync"
	"github.com/heventure/hermes-agent-cluster/internal/telemetry"
	"github.com/heventure/hermes-agent-cluster/internal/visualization"
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

	// --- Initialize telemetry ---
	ctx := context.Background()
	otelProvider, err := telemetry.Init(ctx, cfg.Telemetry)
	if err != nil {
		log.Fatalf("init telemetry: %v", err)
	}
	defer otelProvider.Shutdown(ctx)

	var metricsTelemetry *telemetry.Metrics
	if cfg.Telemetry.IsEnabled() {
		metricsTelemetry, err = telemetry.NewMetrics()
		if err != nil {
			log.Fatalf("init metrics: %v", err)
		}
		log.Printf("telemetry enabled: exporter=%s endpoint=%s", cfg.Telemetry.Exporter, cfg.Telemetry.Endpoint)
	}

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

	// --- Capability change: re-evaluate pending tasks ---
	registry.SetOnCapabilityChange(func(nodeID string, oldCaps, newCaps []string) {
		log.Printf("capability change: node=%s old=%v new=%v", nodeID, oldCaps, newCaps)
		promoted := sched.TriggerPendingTasks()
		if len(promoted) > 0 {
			log.Printf("capability-change trigger: promoted %d pending tasks for node %s", len(promoted), nodeID)
			sched.SchedulePending()
		}
	})

	// --- Recovery: Detector -> Revoker -> Rescheduler ---
	revoker := recovery.NewRevoker(leaseMgr, recLog)
	rescheduler := recovery.NewRescheduler(sched, recLog)
	detector := recovery.NewDetector(revoker, rescheduler, leaseMgr, recLog)

	// --- Auto-reconnect manager (for WAN cluster) ---
	var reconnectMgr *recovery.ReconnectManager
	reconnectMgr = recovery.NewReconnectManager(
		recovery.ReconnectConfig{
			InitialInterval: cfg.Reconnect.InitialInterval,
			MaxInterval:     cfg.Reconnect.MaxInterval,
			Multiplier:      cfg.Reconnect.Multiplier,
		},
		func(target string) error {
			log.Printf("reconnect attempt: target=%s", target)
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(target + "/api/v1/sync/status")
			if err != nil {
				return err
			}
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				log.Printf("reconnect succeeded: target=%s", target)
				reconnectMgr.NotifyConnect(target)
				return nil
			}
			return fmt.Errorf("health check returned HTTP %d", resp.StatusCode)
		},
	)

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

	// --- Prometheus metrics (always enabled, scraped at /metrics) ---
	promMetrics := metrics.New()
	log.Printf("prometheus metrics registered at /metrics")

	// --- Hooks: webhook plugin SDK ---
	hookDispatcher := hooks.NewDispatcher(
		hooks.WithMaxRetries(3),
		hooks.WithBaseDelay(1*time.Second),
		hooks.WithHTTPTimeout(10*time.Second),
		hooks.WithWorkerCount(4),
	)
	hookMgr := hooks.NewManager(hookDispatcher, 1000)
	hookDispatcher.Start()

	// --- Lease expiry callback: triggers recovery on lease expiry ---
	leaseMgr.SetExpiryCallback(func(taskID, nodeID string) {
		log.Printf("lease expired: task=%s node=%s", taskID, nodeID)
		// Mark node as offline in registry so scheduler won't pick it again
		registry.UpdateStatus(nodeID, cluster.NodeOffline)
		detector.NotifyOffline(nodeID)

		// Emit lease_expired event to webhook subscribers
		hookMgr.Emit(hooks.EventLeaseExpired, map[string]string{
			"task_id": taskID,
			"node_id": nodeID,
		})

		// Prometheus: record lease expiry + update active count
		promMetrics.LeaseExpired()
		promMetrics.LeasesActiveUpdate(float64(len(leaseMgr.GetActiveLeases())))
	})

	// --- Cluster visualization ---
	clusterView := visualization.NewClusterView(registry, taskStore, leaseMgr, recLog, resolver)

	// --- Stop channel for background services ---
	stopCh := make(chan struct{})

	// --- Periodic gauge updater: snapshot task/node/lease/sync gauges ---
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Task status counts
				tasks := taskStore.GetAll()
				statusCounts := make(map[string]float64)
				for _, t := range tasks {
					statusCounts[string(t.Status)]++
				}
				for status, count := range statusCounts {
					promMetrics.TaskStatusUpdate(status, count)
				}
				// Node counts
				promMetrics.NodeRegistered(registry.OnlineCount(), registry.Count())
				// Active lease count
				promMetrics.LeasesActiveUpdate(float64(len(leaseMgr.GetActiveLeases())))
				// Sync version
				promMetrics.SyncVersionUpdate(float64(stateStore.Version()))
			case <-stopCh:
				return
			}
		}
	}()

	// --- Federation: cross-cluster discovery and task forwarding ---
	fedRegistry := federation.NewRegistry()
	fedClient := federation.NewClient()
	fedDispatcher := federation.NewDispatcher(fedRegistry, fedClient)
	if cfg.Federation.Enabled {
		fedDispatcher.Start(cfg.Federation.PingInterval)
		log.Printf("federation enabled: health-check interval=%v", cfg.Federation.PingInterval)
	}

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
		api.WithClusterView(clusterView),
		api.WithTelemetry(metricsTelemetry),
		api.WithPromMetrics(promMetrics),
		api.WithHookManager(hookMgr),
		api.WithFederation(fedDispatcher, fedRegistry),
	)

	// --- Start background services ---
	// Start lease expiry scanner
	leaseMgr.StartExpiryScanner(cfg.Lease.ScanRate, stopCh)

	// Start watchdog
	watchdog.Start()

	// Start recovery detector
	detector.Start()

	// Start reconnect manager
	reconnectMgr.Start()
	defer reconnectMgr.Stop()

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

	// Start server in goroutine (TLS or plain HTTP)
	go func() {
		if cfg.TLS.Enabled {
			log.Printf("API listening on %s (TLS)", addr)
			if err := httpServer.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile); err != nil && err != http.ErrServerClosed {
				log.Fatalf("server error: %v", err)
			}
		} else {
			log.Printf("API listening on %s", addr)
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("server error: %v", err)
			}
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
	hookDispatcher.Stop()
	fedDispatcher.Stop()

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

	lastVersion := make(map[string]int64) // per-task last pushed version

	for {
		select {
		case <-ticker.C:
			// Push any tasks with version > lastVersion
			tasks := store.GetAll()
			for _, t := range tasks {
				if t.Version > lastVersion[t.ID] {
					ls.PushTaskState(sync.TaskSync{
						TaskID:     t.ID,
						Title:      t.Title,
						Status:     string(t.Status),
						AssignedTo: t.AssignedTo,
						Version:    t.Version,
					}, sync.EventTaskCreated, senderNode)
					lastVersion[t.ID] = t.Version
				}
			}
		case <-stopCh:
			return
		}
	}
}
