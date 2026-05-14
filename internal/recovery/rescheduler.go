package recovery

import (
	"github.com/heventure/hermes-agent-cluster/internal/scheduler"
)

// Rescheduler handles reassigning tasks after node failure.
type Rescheduler struct {
	scheduler *scheduler.Scheduler
	log       *Log
}

// NewRescheduler creates a task rescheduler.
func NewRescheduler(sched *scheduler.Scheduler, log *Log) *Rescheduler {
	return &Rescheduler{scheduler: sched, log: log}
}

// RescheduleOrphaned attempts to reschedule tasks from a failed node.
// Returns the count of successfully rescheduled tasks.
func (rs *Rescheduler) RescheduleOrphaned(taskIDs []string) int {
	rescheduled := 0
	for _, taskID := range taskIDs {
		newNode, err := rs.scheduler.RescheduleTask(taskID)
		if err != nil {
			rs.log.Append(RecoveryEvent{
				TaskID:  taskID,
				Action:  "reschedule",
				Status:  "failed",
				Message: err.Error(),
			})
			continue
		}
		if newNode != "" {
			rs.log.Append(RecoveryEvent{
				TaskID:  taskID,
				Action:  "reschedule",
				Status:  "completed",
				Message: "rescheduled to " + newNode,
			})
			rescheduled++
		} else {
			// No node available - mark as failed
			rs.scheduler.GetTaskStore().SetStatus(taskID, scheduler.TaskFailed)
			rs.log.Append(RecoveryEvent{
				TaskID:  taskID,
				Action:  "mark_failed",
				Status:  "completed",
				Message: "no available node for reschedule",
			})
		}
	}
	return rescheduled
}
