package recovery

import (
	"github.com/heventure/hermes-agent-cluster/internal/lease"
)

// Revoker revokes all leases for a failed node.
type Revoker struct {
	leaseMgr *lease.Manager
	log      *Log
}

// NewRevoker creates a lease revoker.
func NewRevoker(leaseMgr *lease.Manager, log *Log) *Revoker {
	return &Revoker{leaseMgr: leaseMgr, log: log}
}

// RevokeAllForNode revokes all active leases for a node and logs the events.
func (r *Revoker) RevokeAllForNode(nodeID string) []string {
	revoked := r.leaseMgr.RevokeAllForNode(nodeID)
	for _, taskID := range revoked {
		r.log.Append(RecoveryEvent{
			TaskID: taskID,
			NodeID: nodeID,
			Action: "revoke_lease",
			Status: "completed",
		})
	}
	return revoked
}
