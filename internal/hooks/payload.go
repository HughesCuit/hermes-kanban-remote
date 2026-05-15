package hooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// EventType enumerates the cluster lifecycle events that can trigger webhooks.
type EventType string

const (
	EventTaskCreated   EventType = "task_created"
	EventTaskCompleted EventType = "task_completed"
	EventTaskFailed    EventType = "task_failed"
	EventNodeJoined    EventType = "node_joined"
	EventNodeLeft      EventType = "node_left"
	EventLeaseCreated  EventType = "lease_created"
	EventLeaseExpired  EventType = "lease_expired"
)

// AllEventTypes returns the complete list of supported event types.
func AllEventTypes() []EventType {
	return []EventType{
		EventTaskCreated,
		EventTaskCompleted,
		EventTaskFailed,
		EventNodeJoined,
		EventNodeLeft,
		EventLeaseCreated,
		EventLeaseExpired,
	}
}

// IsValidEvent returns true if the event type is a known cluster event.
func IsValidEvent(et EventType) bool {
	for _, e := range AllEventTypes() {
		if e == et {
			return true
		}
	}
	return false
}

// Payload is the JSON body sent to webhook endpoints.
type Payload struct {
	EventType EventType   `json:"event_type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// DeliveryStatus records the outcome of a single delivery attempt.
type DeliveryStatus string

const (
	DeliverySuccess    DeliveryStatus = "success"
	DeliveryFailed     DeliveryStatus = "failed"
	DeliveryPending    DeliveryStatus = "pending"
	DeliveryRetry      DeliveryStatus = "retry"
)

// Delivery represents one attempt to deliver a webhook payload.
type Delivery struct {
	ID          string         `json:"id"`
	HookID      string         `json:"hook_id"`
	EventType   EventType      `json:"event_type"`
	URL         string         `json:"url"`
	Status      DeliveryStatus `json:"status"`
	StatusCode  int            `json:"status_code,omitempty"`
	Error       string         `json:"error,omitempty"`
	Attempts    int            `json:"attempts"`
	MaxAttempts int            `json:"max_attempts"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// SignPayload computes HMAC-SHA256 over the raw body bytes using the given secret.
// Returns the hex-encoded signature string suitable for the X-Hub-Signature-256 header.
func SignPayload(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return fmt.Sprintf("sha256=%s", hex.EncodeToString(mac.Sum(nil)))
}

// VerifySignature verifies the HMAC-SHA256 signature of raw body bytes.
func VerifySignature(body []byte, secret, signature string) bool {
	expected := SignPayload(body, secret)
	return hmac.Equal([]byte(expected), []byte(signature))
}
