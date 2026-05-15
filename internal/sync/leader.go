package sync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// NodePusher sends sync messages to follower nodes.
type NodePusher interface {
	Push(targetURL string, msg SyncMessage) error
}

// BatchPusher extends NodePusher with batch push support.
type BatchPusher interface {
	NodePusher
	PushBatch(targetURL string, msgs []SyncMessage) error
}

// HTTPPusher implements NodePusher using HTTP POST.
type HTTPPusher struct {
	client *http.Client
}

// NewHTTPPusher creates an HTTP pusher with a custom client.
func NewHTTPPusher() *HTTPPusher {
	return &HTTPPusher{
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// Push sends a sync message to the target URL with exponential backoff retry.
func (p *HTTPPusher) Push(targetURL string, msg SyncMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	var lastErr error
	backoff := 50 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := p.client.Post(targetURL+"/api/v1/sync/receive", "application/json", bytes.NewReader(body))
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("push failed after 3 attempts: %w", lastErr)
}

// LeaderSync pushes state to all follower nodes.
type LeaderSync struct {
	mu          sync.Mutex
	stateStore  *StateStore
	pusher      NodePusher
	followers   []string // base URLs
}

// NewLeaderSync creates a leader sync manager.
func NewLeaderSync(stateStore *StateStore, pusher NodePusher) *LeaderSync {
	return &LeaderSync{
		stateStore: stateStore,
		pusher:     pusher,
	}
}

// AddFollower registers a follower node's base URL.
func (ls *LeaderSync) AddFollower(url string) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.followers = append(ls.followers, url)
}

// RemoveFollower removes a follower by URL.
func (ls *LeaderSync) RemoveFollower(url string) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	for i, f := range ls.followers {
		if f == url {
			ls.followers = append(ls.followers[:i], ls.followers[i+1:]...)
			break
		}
	}
}

// PushTaskState pushes a task state update to all followers.
func (ls *LeaderSync) PushTaskState(taskSync TaskSync, eventType EventType, senderNode string) {
	ls.mu.Lock()
	followers := make([]string, len(ls.followers))
	copy(followers, ls.followers)
	ls.mu.Unlock()

	msg := SyncMessage{
		Version:    taskSync.Version,
		SenderNode: senderNode,
		TaskState:  &taskSync,
		EventType:  eventType,
		Timestamp:  time.Now().UnixMilli(),
	}

	for _, url := range followers {
		go ls.pusher.Push(url, msg)
	}
}

// PushBatch sends multiple sync messages as a batch to the target URL.
func (p *HTTPPusher) PushBatch(targetURL string, msgs []SyncMessage) error {
	batch := BatchSyncMessage{Messages: msgs}
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	var lastErr error
	backoff := 50 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := p.client.Post(targetURL+"/api/v1/sync/receive-batch", "application/json", bytes.NewReader(body))
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("push batch failed after 3 attempts: %w", lastErr)
}

// PushTaskStateBatch collects pending task states and pushes them as a batch to all followers.
func (ls *LeaderSync) PushTaskStateBatch(states []TaskSync, eventType EventType, senderNode string) {
	ls.mu.Lock()
	followers := make([]string, len(ls.followers))
	copy(followers, ls.followers)
	ls.mu.Unlock()

	if len(states) == 0 || len(followers) == 0 {
		return
	}

	msgs := make([]SyncMessage, len(states))
	now := time.Now().UnixMilli()
	for i, ts := range states {
		msgs[i] = SyncMessage{
			Version:    ts.Version,
			SenderNode: senderNode,
			TaskState:  &ts,
			EventType:  eventType,
			Timestamp:  now,
		}
	}

	bp, ok := ls.pusher.(BatchPusher)
	if !ok {
		// Fall back to single-message push
		for _, url := range followers {
			for _, msg := range msgs {
				go ls.pusher.Push(url, msg)
			}
		}
		return
	}

	for _, url := range followers {
		go bp.PushBatch(url, msgs)
	}
}

// FollowerReceiver handles incoming sync messages.
type FollowerReceiver struct {
	stateStore *StateStore
}

// NewFollowerReceiver creates a follower receiver.
func NewFollowerReceiver(stateStore *StateStore) *FollowerReceiver {
	return &FollowerReceiver{stateStore: stateStore}
}

// HandleSyncMessage processes an incoming sync message.
func (fr *FollowerReceiver) HandleSyncMessage(msg SyncMessage) bool {
	return fr.stateStore.Apply(msg)
}

// HandleBatchSyncMessage processes an incoming batch of sync messages.
func (fr *FollowerReceiver) HandleBatchSyncMessage(batch BatchSyncMessage) int {
	return fr.stateStore.ApplyBatch(batch.Messages)
}
