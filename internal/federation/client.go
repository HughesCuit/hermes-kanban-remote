package federation

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is an HTTP client for calling remote cluster APIs.
type Client struct {
	httpClient *http.Client
}

// maxResponseSize caps response body reads to prevent OOM from malicious remote clusters.
const maxResponseSize = 10 << 20 // 10MB

// NewClient creates a new federation HTTP client with sensible defaults.
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// StatusEntry represents a single node/task status entry from a remote cluster.
type StatusEntry struct {
	NodeID     string `json:"node_id"`
	NodeName   string `json:"node_name"`
	Status     string `json:"status"`
	Capability string `json:"capability,omitempty"`
	TaskID     string `json:"task_id,omitempty"`
	TaskTitle  string `json:"task_title,omitempty"`
}

// StatusSummary is the summary of a remote cluster's status.
type StatusSummary struct {
	TotalNodes    int `json:"total_nodes"`
	OnlineNodes   int `json:"online_nodes"`
	TotalTasks    int `json:"total_tasks"`
	RunningTasks  int `json:"running_tasks"`
	CompletedTasks int `json:"completed_tasks"`
}

// StatusResponse is the unified status response from a remote cluster.
// Previously split into PingResponse and RemoteStatusResponse — consolidated
// to eliminate duplicate struct definitions.
type StatusResponse struct {
	Entries []StatusEntry `json:"entries"`
	Summary StatusSummary `json:"summary"`
}

// ForwardTaskRequest is the request body for forwarding a task to a remote cluster.
type ForwardTaskRequest struct {
	Title          string   `json:"title"`
	Requires       []string `json:"requires"`
	IdempotencyKey string   `json:"idempotency_key,omitempty"`
}

// ForwardTaskResponse is the response from a remote cluster after forwarding a task.
type ForwardTaskResponse struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Status string `json:"status"`
}

// generateID produces a short random hex string suitable as an idempotency key.
func generateID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// fallback — should never happen
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// Ping checks if a remote cluster is reachable and returns its status.
// It calls GET /api/v1/status on the remote cluster.
func (c *Client) Ping(endpoint string) (*StatusResponse, time.Duration, error) {
	start := time.Now()
	resp, err := c.httpClient.Get(endpoint + "/api/v1/status")
	latency := time.Since(start)
	if err != nil {
		return nil, latency, fmt.Errorf("ping %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, latency, fmt.Errorf("ping %s: status %d", endpoint, resp.StatusCode)
	}

	var result StatusResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseSize)).Decode(&result); err != nil {
		return nil, latency, fmt.Errorf("ping %s: decode: %w", endpoint, err)
	}
	return &result, latency, nil
}

// ForwardTask submits a task to a remote cluster via POST /api/v1/tasks.
func (c *Client) ForwardTask(endpoint string, title string, requires []string) (*ForwardTaskResponse, error) {
	reqBody := ForwardTaskRequest{
		Title:          title,
		Requires:       requires,
		IdempotencyKey: generateID(),
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal task: %w", err)
	}

	resp, err := c.httpClient.Post(
		endpoint+"/api/v1/tasks",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("forward task to %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
		return nil, fmt.Errorf("forward task to %s: status %d: %s", endpoint, resp.StatusCode, string(respBody))
	}

	var result ForwardTaskResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseSize)).Decode(&result); err != nil {
		return nil, fmt.Errorf("forward task response: %w", err)
	}
	return &result, nil
}

// QueryStatus queries the status of a remote cluster via GET /api/v1/status.
func (c *Client) QueryStatus(endpoint string) (*StatusResponse, error) {
	resp, err := c.httpClient.Get(endpoint + "/api/v1/status")
	if err != nil {
		return nil, fmt.Errorf("query status %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query status %s: status %d", endpoint, resp.StatusCode)
	}

	var result StatusResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseSize)).Decode(&result); err != nil {
		return nil, fmt.Errorf("query status decode: %w", err)
	}
	return &result, nil
}
