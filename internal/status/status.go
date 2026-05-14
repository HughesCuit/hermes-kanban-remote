package status

import (
	"sort"

	"github.com/heventure/hermes-agent-cluster/internal/cluster"
	"github.com/heventure/hermes-agent-cluster/internal/lease"
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

// StatusEntry represents a single task in the global status view.
type StatusEntry struct {
	TaskID      string              `json:"task_id"`
	TaskTitle   string              `json:"task_title"`
	TaskStatus  scheduler.TaskStatus `json:"task_status"`
	NodeID      string              `json:"node_id"`
	NodeName    string              `json:"node_name"`
	NodeStatus  cluster.NodeStatus  `json:"node_status"`
	Capabilities []string           `json:"capabilities,omitempty"`
	Requires    []string            `json:"requires,omitempty"`
	LeaseStatus string              `json:"lease_status,omitempty"`
	FailReason  string              `json:"fail_reason,omitempty"`
}

// Summary provides aggregate counts for the global status view.
type Summary struct {
	TotalNodes   int                          `json:"total_nodes"`
	OnlineNodes  int                          `json:"online_nodes"`
	TotalTasks   int                          `json:"total_tasks"`
	TasksByStatus map[string]int              `json:"tasks_by_status"`
	ActiveLeases int                          `json:"active_leases"`
}

// StatusView aggregates all cluster state into a queryable view.
type StatusView struct {
	Registry  *cluster.Registry
	TaskStore *scheduler.TaskStore
	LeaseMgr  *lease.Manager
}

// NewStatusView creates a new StatusView.
func NewStatusView(registry *cluster.Registry, taskStore *scheduler.TaskStore, leaseMgr *lease.Manager) *StatusView {
	return &StatusView{
		Registry:  registry,
		TaskStore: taskStore,
		LeaseMgr:  leaseMgr,
	}
}

// Filter holds optional query parameters for filtering the status view.
type Filter struct {
	NodeID     string // filter by node ID
	Status     string // filter by task status
	Capability string // filter by capability (any node with this cap)
}

// Query returns the filtered status view and summary.
func (sv *StatusView) Query(f Filter) ([]StatusEntry, Summary) {
	tasks := sv.TaskStore.GetAll()
	nodes := sv.Registry.GetAll()
	activeLeases := sv.LeaseMgr.GetActiveLeases()

	// Build lookup maps
	nodeMap := make(map[string]*cluster.Node, len(nodes))
	for _, n := range nodes {
		nodeMap[n.ID] = n
	}
	leaseByTask := make(map[string]*lease.Lease, len(activeLeases))
	for _, l := range activeLeases {
		leaseByTask[l.TaskID] = l
	}

	// Build summary (unfiltered)
	summary := Summary{
		TotalNodes:   len(nodes),
		OnlineNodes:  sv.Registry.OnlineCount(),
		TotalTasks:   len(tasks),
		TasksByStatus: make(map[string]int),
		ActiveLeases: len(activeLeases),
	}
	for _, t := range tasks {
		summary.TasksByStatus[string(t.Status)]++
	}

	// Build entries with filtering
	var entries []StatusEntry
	for _, t := range tasks {
		// Filter by task status
		if f.Status != "" && string(t.Status) != f.Status {
			continue
		}

		entry := StatusEntry{
			TaskID:     t.ID,
			TaskTitle:  t.Title,
			TaskStatus: t.Status,
			Requires:   t.Requires,
			FailReason: t.FailReason,
		}

		// Resolve node info
		if t.AssignedTo != "" {
			if node, ok := nodeMap[t.AssignedTo]; ok {
				entry.NodeID = node.ID
				entry.NodeName = node.Name
				entry.NodeStatus = node.Status
				entry.Capabilities = node.Capabilities
			} else {
				entry.NodeID = t.AssignedTo
			}
		}

		// Attach lease info
		if l, ok := leaseByTask[t.ID]; ok {
			entry.LeaseStatus = string(l.Status)
		}

		// Filter by node ID
		if f.NodeID != "" && entry.NodeID != f.NodeID {
			continue
		}

		// Filter by capability
		if f.Capability != "" {
			hasCap := false
			for _, c := range entry.Capabilities {
				if c == f.Capability {
					hasCap = true
					break
				}
			}
			if !hasCap {
				continue
			}
		}

		entries = append(entries, entry)
	}

	// Sort by task status then task ID for stable output
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].TaskStatus != entries[j].TaskStatus {
			return entries[i].TaskStatus < entries[j].TaskStatus
		}
		return entries[i].TaskID < entries[j].TaskID
	})

	return entries, summary
}
