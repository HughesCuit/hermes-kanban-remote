package recovery

import (
	"sync"
	"time"

	"github.com/heventure/hermes-agent-cluster/internal/lease"
)

// OfflineEvent represents a node going offline.
type OfflineEvent struct {
	NodeID    string
	Timestamp time.Time
}

// Detector watches for offline events and triggers recovery.
type Detector struct {
	mu          sync.Mutex
	revoker     *Revoker
	rescheduler *Rescheduler
	leaseMgr    *lease.Manager
	log         *Log
	running     bool
	stopCh      chan struct{}
	eventCh     chan OfflineEvent
}

// NewDetector creates a recovery detector.
func NewDetector(revoker *Revoker, rescheduler *Rescheduler, leaseMgr *lease.Manager, log *Log) *Detector {
	return &Detector{
		revoker:     revoker,
		rescheduler: rescheduler,
		leaseMgr:    leaseMgr,
		log:         log,
		eventCh:     make(chan OfflineEvent, 100),
	}
}

// Start begins the detector loop.
func (d *Detector) Start() {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return
	}
	d.running = true
	d.stopCh = make(chan struct{})
	d.mu.Unlock()

	go func() {
		for {
			select {
			case evt := <-d.eventCh:
				d.handleOffline(evt)
			case <-d.stopCh:
				return
			}
		}
	}()
}

// Stop halts the detector.
func (d *Detector) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.running {
		return
	}
	d.running = false
	close(d.stopCh)
}

// NotifyOffline sends an offline event to the detector.
func (d *Detector) NotifyOffline(nodeID string) {
	d.eventCh <- OfflineEvent{NodeID: nodeID, Timestamp: time.Now()}
}

// handleOffline performs the recovery sequence for a failed node.
func (d *Detector) handleOffline(evt OfflineEvent) {
	// 1. Revoke all leases for the failed node
	revokedTaskIDs := d.revoker.RevokeAllForNode(evt.NodeID)

	// 2. Try to reschedule orphaned tasks
	if len(revokedTaskIDs) > 0 {
		rescheduled := d.rescheduler.RescheduleOrphaned(revokedTaskIDs)
		status := "completed"
		if rescheduled < len(revokedTaskIDs) {
			status = "partial"
		}
		d.log.Append(RecoveryEvent{
			NodeID:  evt.NodeID,
			Action:  "full_recovery",
			Status:  status,
			Message: "revoked and rescheduled",
		})
	}
}
