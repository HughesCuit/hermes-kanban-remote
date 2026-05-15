package sync

import (
	"testing"
)

func TestApplyBatch_Empty(t *testing.T) {
	ss := NewStateStore()
	applied := ss.ApplyBatch(nil)
	if applied != 0 {
		t.Errorf("expected 0 applied, got %d", applied)
	}
}

func TestApplyBatch_MultipleMessages(t *testing.T) {
	ss := NewStateStore()
	msgs := []SyncMessage{
		{Version: 1, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t1", Title: "task1", Status: "pending", Version: 1}},
		{Version: 2, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t2", Title: "task2", Status: "running", Version: 2}},
		{Version: 3, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t3", Title: "task3", Status: "completed", Version: 3}},
	}
	applied := ss.ApplyBatch(msgs)
	if applied != 3 {
		t.Errorf("expected 3 applied, got %d", applied)
	}
	if ss.Version() != 3 {
		t.Errorf("expected version 3, got %d", ss.Version())
	}
}

func TestApplyBatch_StaleSkipped(t *testing.T) {
	ss := NewStateStore()
	// Apply initial message
	ss.Apply(SyncMessage{Version: 2, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t1", Title: "task1", Status: "pending", Version: 2}})

	// Batch with one stale and one new
	msgs := []SyncMessage{
		{Version: 1, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t1", Title: "old", Status: "old", Version: 1}},
		{Version: 3, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t2", Title: "task2", Status: "running", Version: 3}},
	}
	applied := ss.ApplyBatch(msgs)
	if applied != 1 {
		t.Errorf("expected 1 applied (stale skipped), got %d", applied)
	}
	ts, ok := ss.Get("t1")
	if !ok || ts.Title != "task1" {
		t.Error("stale message should not have overwritten existing state")
	}
}

func TestApplyBatch_NilTaskStateSkipped(t *testing.T) {
	ss := NewStateStore()
	msgs := []SyncMessage{
		{Version: 1, SenderNode: "node1", TaskState: nil},
		{Version: 2, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t1", Title: "task1", Status: "pending", Version: 2}},
	}
	applied := ss.ApplyBatch(msgs)
	if applied != 1 {
		t.Errorf("expected 1 applied (nil skipped), got %d", applied)
	}
}

func TestApplyBatch_VersionTracking(t *testing.T) {
	ss := NewStateStore()
	msgs := []SyncMessage{
		{Version: 5, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t1", Title: "task1", Status: "pending", Version: 5}},
		{Version: 3, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t2", Title: "task2", Status: "running", Version: 3}},
	}
	ss.ApplyBatch(msgs)
	// Global version should be max of applied versions
	if ss.Version() != 5 {
		t.Errorf("expected version 5, got %d", ss.Version())
	}
}

func TestBatchSyncMessage_Structure(t *testing.T) {
	batch := BatchSyncMessage{
		Messages: []SyncMessage{
			{Version: 1, SenderNode: "node1", TaskState: &TaskSync{TaskID: "t1", Version: 1}},
		},
	}
	if len(batch.Messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(batch.Messages))
	}
}
