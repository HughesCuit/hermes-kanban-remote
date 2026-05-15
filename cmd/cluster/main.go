package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
	// Remove "cluster" binary name if present as first arg
	args := os.Args[1:]

	// Detect subcommand
	if len(args) > 0 {
		cmd := args[0]
		switch cmd {
		case "help", "--help", "-h":
			printUsage()
			return
		case "status":
			cmdStatus(args[1:])
			return
		case "health":
			cmdHealth(args[1:])
			return
		case "config":
			if len(args) > 1 {
				switch args[1] {
				case "init":
					cmdConfigInit(args[2:])
					return
				case "validate":
					cmdConfigValidate(args[2:])
					return
				default:
					fmt.Fprintf(os.Stderr, "unknown config subcommand: %s\n\n", args[1])
					printUsage()
					os.Exit(1)
				}
			}
			fmt.Fprintln(os.Stderr, "config requires a subcommand: init or validate")
			printUsage()
			os.Exit(1)
		case "serve":
			cmdServe(args[1:])
			return
		default:
			fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
			printUsage()
			os.Exit(1)
		}
	}

	// No subcommand → default to serve
	cmdServe(nil)
}

// printUsage displays CLI usage information.
func printUsage() {
	fmt.Println("hermes-agent-cluster — distributed cluster manager")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  cluster [command]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  serve              Start the cluster server (default)")
	fmt.Println("  status             Show cluster status from a running node")
	fmt.Println("  health             Check health of a running node")
	fmt.Println("  config init        Generate a default config file")
	fmt.Println("  config validate    Validate a config file")
	fmt.Println("  help               Show this help message")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  --config    Path to config file (default: cluster.yaml)")
	fmt.Println("  --url       API server URL (default: http://localhost:8787)")
	fmt.Println("  --output    Output file path (for config init)")
}

// --- cmdServe starts the cluster server (original main() logic) ---

func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", "cluster.yaml", "path to cluster.yaml config file")
	fs.Parse(args)

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
		api.WithConfig(cfg, *configPath),
		api.WithClusterView(clusterView),
		api.WithTelemetry(metricsTelemetry),
		api.WithPromMetrics(promMetrics),
		api.WithHookManager(hookMgr),
		api.WithFederation(fedDispatcher, fedRegistry, cfg.Federation.Token),
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

// --- cmdStatus fetches cluster status from a running node ---

func cmdStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	url := fs.String("url", "http://localhost:8787", "API server URL")
	fs.Parse(args)

	client := &http.Client{Timeout: 5 * time.Second}

	// Fetch health
	healthResp, healthErr := client.Get(*url + "/health")
	healthData := make(map[string]interface{})
	if healthErr != nil {
		fmt.Fprintf(os.Stderr, "error: cannot reach cluster at %s: %v\n", *url, healthErr)
		os.Exit(1)
	}
	defer healthResp.Body.Close()
	json.NewDecoder(healthResp.Body).Decode(&healthData)

	// Fetch summary
	summaryResp, summaryErr := client.Get(*url + "/api/v1/summary")
	summaryData := make(map[string]interface{})
	if summaryErr != nil {
		fmt.Fprintf(os.Stderr, "error: cannot fetch summary: %v\n", summaryErr)
		os.Exit(1)
	}
	defer summaryResp.Body.Close()
	json.NewDecoder(summaryResp.Body).Decode(&summaryData)

	// Format output
	fmt.Println()
	if clusterID, ok := summaryData["cluster_id"].(string); ok {
		role := ""
		if r, ok := summaryData["role"].(string); ok {
			role = r
		}
		if nodeID, ok := summaryData["node_id"].(string); ok {
			fmt.Printf("Cluster: %s (role: %s)\n", clusterID, role)
			fmt.Printf("Node:    %s\n", nodeID)
		} else {
			fmt.Printf("Cluster: %s (role: %s)\n", clusterID, role)
		}
	}

	// Uptime
	if uptime, ok := healthData["uptime"].(string); ok {
		fmt.Printf("Uptime:  %s\n", uptime)
	} else if started, ok := healthData["started"].(string); ok {
		fmt.Printf("Started: %s\n", started)
	}
	fmt.Println()

	// Nodes
	if nodes, ok := summaryData["nodes"].(map[string]interface{}); ok {
		totalNodes, _ := nodes["total"].(float64)
		onlineNodes, _ := nodes["online"].(float64)
		fmt.Printf("Nodes:   %d total, %d online\n", int(totalNodes), int(onlineNodes))
	}

	// Tasks
	if tasks, ok := summaryData["tasks"].(map[string]interface{}); ok {
		totalTasks, _ := tasks["total"].(float64)
		parts := []string{}
		for _, status := range []string{"ready", "running", "completed", "failed", "pending"} {
			if c, ok := tasks[status].(float64); ok && c > 0 {
				parts = append(parts, fmt.Sprintf("%d %s", int(c), status))
			}
		}
		if len(parts) > 0 {
			fmt.Printf("Tasks:   %d total (%s)\n", int(totalTasks), strings.Join(parts, ", "))
		} else {
			fmt.Printf("Tasks:   %d total\n", int(totalTasks))
		}
	}

	// Leases
	if leases, ok := summaryData["leases"].(map[string]interface{}); ok {
		if activeLeases, ok := leases["active"].(float64); ok {
			fmt.Printf("Leases:  %d active\n", int(activeLeases))
		}
	}

	// Sync version
	if syncVersion, ok := summaryData["sync_version"].(float64); ok {
		fmt.Printf("Sync:    v%d\n", int(syncVersion))
	}

	fmt.Println()
}

// --- cmdHealth checks health of a running node ---

func cmdHealth(args []string) {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	url := fs.String("url", "http://localhost:8787", "API server URL")
	fs.Parse(args)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(*url + "/health")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot reach %s: %v\n", *url, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("OK (HTTP %d) — %s\n", resp.StatusCode, *url)
		// Pretty-print JSON if present
		var data map[string]interface{}
		if json.Unmarshal(body, &data) == nil {
			for k, v := range data {
				fmt.Printf("  %s: %v\n", k, v)
			}
		}
	} else {
		fmt.Fprintf(os.Stderr, "FAIL (HTTP %d) — %s\n", resp.StatusCode, *url)
		os.Exit(1)
	}
}

// --- cmdConfigInit generates a default config file ---

func cmdConfigInit(args []string) {
	fs := flag.NewFlagSet("config init", flag.ExitOnError)
	output := fs.String("output", "cluster.yaml", "output file path")
	fs.Parse(args)

	cfg := config.DefaultConfig()
	if err := config.Save(cfg, *output); err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot write config: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Default config written to %s\n", *output)
	fmt.Println("Edit the file to customize your cluster settings.")
}

// --- cmdConfigValidate validates a config file ---

func cmdConfigValidate(args []string) {
	fs := flag.NewFlagSet("config validate", flag.ExitOnError)
	configPath := fs.String("config", "cluster.yaml", "path to config file")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot load config: %v\n", err)
		os.Exit(1)
	}

	errs := cfg.ValidateDetailed()
	if len(errs) == 0 {
		fmt.Printf("Config %s: OK\n", *configPath)
		fmt.Printf("  cluster.id: %s\n", cfg.Cluster.ID)
		fmt.Printf("  node.id:    %s\n", cfg.Node.ID)
		fmt.Printf("  server:     %s\n", cfg.Server.BindAddress())
		fmt.Printf("  role:       %s\n", cfg.Cluster.Role)
		return
	}

	fmt.Fprintf(os.Stderr, "Config %s: %d error(s)\n\n", *configPath, len(errs))
	for i, e := range errs {
		fmt.Fprintf(os.Stderr, "  %d. [%s] %s\n", i+1, e.Field, e.Message)
		if e.Suggestion != "" {
			fmt.Fprintf(os.Stderr, "     suggestion: %s\n", e.Suggestion)
		}
	}
	fmt.Println()
	os.Exit(1)
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
