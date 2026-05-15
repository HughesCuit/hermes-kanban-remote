package hooks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sync"
	"time"
)

const (
	// DefaultMaxRetries is the default number of delivery retry attempts.
	DefaultMaxRetries = 3
	// DefaultBaseDelay is the base delay for exponential backoff.
	DefaultBaseDelay = 1 * time.Second
	// DefaultMaxDelay caps the exponential backoff delay.
	DefaultMaxDelay = 30 * time.Second
	// DefaultHTTPTimeout is the timeout for HTTP delivery requests.
	DefaultHTTPTimeout = 10 * time.Second
	// DefaultWorkerCount is the number of concurrent delivery workers.
	DefaultWorkerCount = 4
)

// DeliveryCallback is called after each delivery attempt (success or failure).
type DeliveryCallback func(d *Delivery)

// Dispatcher handles async webhook delivery with exponential backoff retries.
type Dispatcher struct {
	client     *http.Client
	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
	workers    int
	wg         sync.WaitGroup
	stopCh     chan struct{}
}

// DispatcherOption configures the Dispatcher.
type DispatcherOption func(*Dispatcher)

// WithMaxRetries sets the maximum number of retry attempts per delivery.
func WithMaxRetries(n int) DispatcherOption {
	return func(d *Dispatcher) { d.maxRetries = n }
}

// WithBaseDelay sets the base delay for exponential backoff.
func WithBaseDelay(delay time.Duration) DispatcherOption {
	return func(d *Dispatcher) { d.baseDelay = delay }
}

// WithHTTPTimeout sets the HTTP client timeout.
func WithHTTPTimeout(timeout time.Duration) DispatcherOption {
	return func(d *Dispatcher) { d.client.Timeout = timeout }
}

// WithWorkerCount sets the number of concurrent delivery goroutines.
func WithWorkerCount(n int) DispatcherOption {
	return func(d *Dispatcher) { d.workers = n }
}

// NewDispatcher creates a new delivery dispatcher with the given options.
func NewDispatcher(opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		client:     &http.Client{Timeout: DefaultHTTPTimeout},
		maxRetries: DefaultMaxRetries,
		baseDelay:  DefaultBaseDelay,
		maxDelay:   DefaultMaxDelay,
		workers:    DefaultWorkerCount,
		stopCh:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Start launches the background delivery worker pool.
func (d *Dispatcher) Start() {
	// Workers are started on-demand in Deliver; Start just marks as ready.
	log.Printf("hooks dispatcher started: workers=%d max_retries=%d", d.workers, d.maxRetries)
}

// Stop signals workers to stop and waits for in-flight deliveries.
func (d *Dispatcher) Stop() {
	close(d.stopCh)
	d.wg.Wait()
	log.Printf("hooks dispatcher stopped")
}

// Deliver enqueues a payload for async delivery to the given hook.
// It returns immediately; delivery runs in a background goroutine.
func (d *Dispatcher) Deliver(hook Hook, payload Payload, callback DeliveryCallback) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.deliverWithRetry(hook, payload, callback)
	}()
}

// deliverWithRetry sends the payload with exponential backoff retries.
func (d *Dispatcher) deliverWithRetry(hook Hook, payload Payload, callback DeliveryCallback) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("hooks: failed to marshal payload for hook %s: %v", hook.ID, err)
		d.recordAttempt(hook, payload.EventType, DeliveryFailed, 0, 0, err.Error(), callback)
		return
	}

	deliveryID := generateID("deliv")

	for attempt := 1; attempt <= d.maxRetries+1; attempt++ {
		// Check for shutdown
		select {
		case <-d.stopCh:
			d.recordAttempt(hook, payload.EventType, DeliveryPending, attempt, d.maxRetries+1, "dispatcher stopped", callback)
			return
		default:
		}

		statusCode, err := d.sendRequest(hook, body)

		if err == nil && statusCode >= 200 && statusCode < 300 {
			// Success
			d.recordAttemptWithID(deliveryID, hook, payload.EventType, DeliverySuccess, statusCode, attempt, d.maxRetries+1, "", callback)
			return
		}

		// Determine error message
		errMsg := fmt.Sprintf("status %d", statusCode)
		if err != nil {
			errMsg = err.Error()
		}

		// If this was the last attempt, record failure
		if attempt > d.maxRetries {
			d.recordAttemptWithID(deliveryID, hook, payload.EventType, DeliveryFailed, statusCode, attempt, d.maxRetries+1, errMsg, callback)
			return
		}

		// Exponential backoff before retry
		delay := d.computeBackoff(attempt)
		d.recordAttemptWithID(deliveryID, hook, payload.EventType, DeliveryRetry, statusCode, attempt, d.maxRetries+1, errMsg, callback)

		select {
		case <-time.After(delay):
		case <-d.stopCh:
			d.recordAttemptWithID(deliveryID, hook, payload.EventType, DeliveryPending, statusCode, attempt, d.maxRetries+1, "dispatcher stopped during retry", callback)
			return
		}
	}
}

// sendRequest performs the actual HTTP POST to the webhook endpoint.
// Returns the status code and any error.
func (d *Dispatcher) sendRequest(hook Hook, body []byte) (int, error) {
	req, err := http.NewRequest(http.MethodPost, hook.URL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "hermes-agent-cluster/1.0")
	req.Header.Set("X-Hook-ID", hook.ID)

	// HMAC-SHA256 signature if secret is configured
	if hook.Secret != "" {
		signature := SignPayload(body, hook.Secret)
		req.Header.Set("X-Hub-Signature-256", signature)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused
	io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

// computeBackoff calculates the delay for the given attempt (1-indexed).
// Uses exponential backoff with jitter, capped at maxDelay.
func (d *Dispatcher) computeBackoff(attempt int) time.Duration {
	// 2^(attempt-1) * baseDelay
	delay := float64(d.baseDelay) * math.Pow(2, float64(attempt-1))
	if delay > float64(d.maxDelay) {
		delay = float64(d.maxDelay)
	}
	return time.Duration(delay)
}

// recordAttempt is a convenience wrapper around recordAttemptWithID.
func (d *Dispatcher) recordAttempt(hook Hook, eventType EventType, status DeliveryStatus, attempts, maxAttempts int, errMsg string, callback DeliveryCallback) {
	d.recordAttemptWithID(generateID("deliv"), hook, eventType, status, 0, attempts, maxAttempts, errMsg, callback)
}

// recordAttemptWithID creates a Delivery record and invokes the callback.
func (d *Dispatcher) recordAttemptWithID(id string, hook Hook, eventType EventType, status DeliveryStatus, statusCode, attempts, maxAttempts int, errMsg string, callback DeliveryCallback) {
	now := time.Now()
	delivery := &Delivery{
		ID:          id,
		HookID:      hook.ID,
		EventType:   eventType,
		URL:         hook.URL,
		Status:      status,
		StatusCode:  statusCode,
		Error:       errMsg,
		Attempts:    attempts,
		MaxAttempts: maxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if callback != nil {
		callback(delivery)
	}
}
