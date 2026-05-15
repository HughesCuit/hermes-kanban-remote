package hooks

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Payload tests ---

func TestIsValidEvent(t *testing.T) {
	tests := []struct {
		input EventType
		valid bool
	}{
		{EventTaskCreated, true},
		{EventTaskCompleted, true},
		{EventTaskFailed, true},
		{EventNodeJoined, true},
		{EventNodeLeft, true},
		{EventLeaseCreated, true},
		{EventLeaseExpired, true},
		{"unknown_event", false},
		{"", false},
	}
	for _, tt := range tests {
		got := IsValidEvent(tt.input)
		if got != tt.valid {
			t.Errorf("IsValidEvent(%q) = %v, want %v", tt.input, got, tt.valid)
		}
	}
}

func TestAllEventTypes(t *testing.T) {
	events := AllEventTypes()
	if len(events) != 7 {
		t.Errorf("AllEventTypes() returned %d events, want 7", len(events))
	}
	seen := make(map[EventType]bool)
	for _, e := range events {
		if seen[e] {
			t.Errorf("duplicate event type: %s", e)
		}
		seen[e] = true
	}
}

func TestSignAndVerifyPayload(t *testing.T) {
	body := []byte(`{"event_type":"task_created","data":{}}`)
	secret := "my_test_secret"

	sig := SignPayload(body, secret)
	if sig == "" {
		t.Fatal("SignPayload returned empty string")
	}

	if !VerifySignature(body, secret, sig) {
		t.Fatal("VerifySignature failed for valid signature")
	}

	// Wrong secret
	if VerifySignature(body, "wrong_secret", sig) {
		t.Fatal("VerifySignature should fail with wrong secret")
	}

	// Tampered body
	tampered := []byte(`{"event_type":"task_completed","data":{}}`)
	if VerifySignature(tampered, secret, sig) {
		t.Fatal("VerifySignature should fail with tampered body")
	}
}

// --- Manager tests ---

func TestManagerRegister(t *testing.T) {
	m := newTestManager(t)

	h, err := m.Register("https://example.com/hook", []EventType{EventTaskCreated}, "secret123")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if h.ID == "" {
		t.Fatal("Register returned hook with empty ID")
	}
	if h.URL != "https://example.com/hook" {
		t.Errorf("URL = %q, want %q", h.URL, "https://example.com/hook")
	}
	if len(h.Events) != 1 || h.Events[0] != EventTaskCreated {
		t.Errorf("Events = %v, want [task_created]", h.Events)
	}
	if !h.Active {
		t.Error("hook should be active")
	}
}

func TestManagerRegisterValidation(t *testing.T) {
	m := newTestManager(t)

	// Empty URL
	_, err := m.Register("", []EventType{EventTaskCreated}, "")
	if err == nil {
		t.Fatal("expected error for empty URL")
	}

	// No events
	_, err = m.Register("https://example.com", nil, "")
	if err == nil {
		t.Fatal("expected error for no events")
	}

	// Invalid event type
	_, err = m.Register("https://example.com", []EventType{"invalid"}, "")
	if err == nil {
		t.Fatal("expected error for invalid event type")
	}
}

func TestManagerDeregister(t *testing.T) {
	m := newTestManager(t)
	h, _ := m.Register("https://example.com/hook", []EventType{EventTaskCreated}, "")

	err := m.Deregister(h.ID)
	if err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// Should be gone
	_, ok := m.Get(h.ID)
	if ok {
		t.Fatal("hook should be deregistered")
	}

	// Deregister non-existent
	err = m.Deregister("nonexistent")
	if err == nil {
		t.Fatal("expected error deregistering nonexistent hook")
	}
}

func TestManagerList(t *testing.T) {
	m := newTestManager(t)
	m.Register("https://example.com/h1", []EventType{EventTaskCreated}, "secret1")
	m.Register("https://example.com/h2", []EventType{EventNodeJoined}, "secret2")

	hooks := m.List()
	if len(hooks) != 2 {
		t.Fatalf("List returned %d hooks, want 2", len(hooks))
	}

	// Secrets should be omitted in list
	for _, h := range hooks {
		if h.Secret != "" {
			t.Error("List should not include secrets")
		}
	}
}

func TestManagerGetHooksForEvent(t *testing.T) {
	m := newTestManager(t)
	m.Register("https://example.com/tasks", []EventType{EventTaskCreated, EventTaskCompleted}, "")
	m.Register("https://example.com/nodes", []EventType{EventNodeJoined}, "")

	taskHooks := m.GetHooksForEvent(EventTaskCreated)
	if len(taskHooks) != 1 {
		t.Fatalf("GetHooksForEvent(task_created) = %d, want 1", len(taskHooks))
	}

	nodeHooks := m.GetHooksForEvent(EventNodeJoined)
	if len(nodeHooks) != 1 {
		t.Fatalf("GetHooksForEvent(node_joined) = %d, want 1", len(nodeHooks))
	}

	// No hooks for node_left
	leftHooks := m.GetHooksForEvent(EventNodeLeft)
	if len(leftHooks) != 0 {
		t.Fatalf("GetHooksForEvent(node_left) = %d, want 0", len(leftHooks))
	}
}

func TestManagerGetDeliveries(t *testing.T) {
	m := newTestManager(t)
	h, _ := m.Register("https://example.com/hook", []EventType{EventTaskCreated}, "")

	// Manually record some deliveries
	m.recordDelivery(&Delivery{ID: "d1", HookID: h.ID, Status: DeliverySuccess})
	m.recordDelivery(&Delivery{ID: "d2", HookID: h.ID, Status: DeliveryFailed})
	m.recordDelivery(&Delivery{ID: "d3", HookID: "other_hook", Status: DeliverySuccess})

	deliveries := m.GetDeliveries(h.ID)
	if len(deliveries) != 2 {
		t.Fatalf("GetDeliveries returned %d, want 2", len(deliveries))
	}
}

func TestManagerDeliveryHistoryCap(t *testing.T) {
	m := &Manager{
		hooks:      make(map[string]*Hook),
		deliveries: make([]*Delivery, 0, 5),
		maxHistory: 5,
		dispatcher: NewDispatcher(),
	}

	for i := 0; i < 10; i++ {
		m.recordDelivery(&Delivery{ID: generateID("d")})
	}

	if len(m.deliveries) != 5 {
		t.Errorf("delivery history = %d, want capped at 5", len(m.deliveries))
	}
}

// --- Dispatcher tests ---

func TestDispatcherDeliverSuccess(t *testing.T) {
	var received bool
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		received = true

		// Verify headers
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("X-Hook-ID") == "" {
			t.Error("X-Hook-ID header missing")
		}

		// Verify payload
		body, _ := io.ReadAll(r.Body)
		var p Payload
		if err := json.Unmarshal(body, &p); err != nil {
			t.Errorf("failed to unmarshal payload: %v", err)
		}
		if p.EventType != EventTaskCreated {
			t.Errorf("EventType = %q, want %q", p.EventType, EventTaskCreated)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(WithMaxRetries(0), WithBaseDelay(10*time.Millisecond))
	var callbackCalled int32
	d.Deliver(
		Hook{ID: "hook_1", URL: server.URL, Active: true},
		Payload{EventType: EventTaskCreated, Data: map[string]string{"task_id": "t1"}},
		func(del *Delivery) {
			atomic.AddInt32(&callbackCalled, 1)
		},
	)

	// Wait for delivery
	d.wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if !received {
		t.Error("server did not receive the webhook")
	}
	if atomic.LoadInt32(&callbackCalled) != 1 {
		t.Errorf("callback called %d times, want 1", atomic.LoadInt32(&callbackCalled))
	}
}

func TestDispatcherDeliverWithSignature(t *testing.T) {
	secret := "test_secret_123"
	var receivedSig string
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get("X-Hub-Signature-256")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(WithMaxRetries(0))
	d.Deliver(
		Hook{ID: "hook_1", URL: server.URL, Secret: secret, Active: true},
		Payload{EventType: EventTaskCreated},
		nil,
	)
	d.wg.Wait()

	// Verify signature is valid for the actual body bytes
	expectedSig := SignPayload(receivedBody, secret)
	if receivedSig != expectedSig {
		t.Errorf("signature = %q, want %q", receivedSig, expectedSig)
	}
	// Also verify it's not empty
	if receivedSig == "" {
		t.Error("signature should not be empty")
	}
}

func TestDispatcherDeliverRetry(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(
		WithMaxRetries(3),
		WithBaseDelay(10*time.Millisecond),
	)

	var finalStatus DeliveryStatus
	d.Deliver(
		Hook{ID: "hook_retry", URL: server.URL, Active: true},
		Payload{EventType: EventTaskCreated},
		func(del *Delivery) {
			if del.Status == DeliverySuccess || del.Status == DeliveryFailed {
				finalStatus = del.Status
			}
		},
	)
	d.wg.Wait()

	gotAttempts := atomic.LoadInt32(&attempts)
	if gotAttempts != 3 {
		t.Errorf("server received %d attempts, want 3", gotAttempts)
	}
	if finalStatus != DeliverySuccess {
		t.Errorf("final status = %q, want %q", finalStatus, DeliverySuccess)
	}
}

func TestDispatcherDeliverMaxRetriesExhausted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	d := NewDispatcher(
		WithMaxRetries(2),
		WithBaseDelay(10*time.Millisecond),
	)

	var finalStatus DeliveryStatus
	d.Deliver(
		Hook{ID: "hook_exhausted", URL: server.URL, Active: true},
		Payload{EventType: EventTaskCreated},
		func(del *Delivery) {
			finalStatus = del.Status
		},
	)
	d.wg.Wait()

	if finalStatus != DeliveryFailed {
		t.Errorf("final status = %q, want %q (after exhausting retries)", finalStatus, DeliveryFailed)
	}
}

func TestDispatcherDeliverConnectionRefused(t *testing.T) {
	d := NewDispatcher(
		WithMaxRetries(1),
		WithBaseDelay(10*time.Millisecond),
	)

	var finalErr string
	d.Deliver(
		Hook{ID: "hook_bad", URL: "http://127.0.0.1:1", Active: true},
		Payload{EventType: EventTaskCreated},
		func(del *Delivery) {
			if del.Status == DeliveryFailed {
				finalErr = del.Error
			}
		},
	)
	d.wg.Wait()

	if finalErr == "" {
		t.Error("expected error message for connection refused")
	}
}

func TestDispatcherComputeBackoff(t *testing.T) {
	d := NewDispatcher(WithBaseDelay(1*time.Second))
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},   // 2^0 * 1s
		{2, 2 * time.Second},   // 2^1 * 1s
		{3, 4 * time.Second},   // 2^2 * 1s
		{4, 8 * time.Second},   // 2^3 * 1s
		{5, 16 * time.Second},  // 2^4 * 1s
		{6, 30 * time.Second},  // capped at maxDelay
	}
	for _, tt := range tests {
		got := d.computeBackoff(tt.attempt)
		if got != tt.want {
			t.Errorf("computeBackoff(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestDispatcherStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(WithMaxRetries(0), WithBaseDelay(50*time.Millisecond))

	d.Deliver(
		Hook{ID: "hook_stop", URL: server.URL, Active: true},
		Payload{EventType: EventTaskCreated},
		nil,
	)

	// Stop immediately — should not panic
	d.Stop()
}

// --- HMAC signing tests ---

func TestHMACRoundTrip(t *testing.T) {
	body := []byte(`{"test":"data"}`)
	secret := "hmac_secret"

	sig := SignPayload(body, secret)
	if !VerifySignature(body, secret, sig) {
		t.Fatal("HMAC round-trip failed")
	}

	// Verify signature format
	if len(sig) < 7 || sig[:7] != "sha256=" {
		t.Errorf("signature format = %q, want sha256=...", sig)
	}
}

func TestHMACDifferentSecrets(t *testing.T) {
	body := []byte(`{"test":"data"}`)
	sig1 := SignPayload(body, "secret1")
	sig2 := SignPayload(body, "secret2")
	if sig1 == sig2 {
		t.Error("different secrets produced same signature")
	}
}

func TestHMACDifferentBodies(t *testing.T) {
	secret := "same_secret"
	sig1 := SignPayload([]byte(`body1`), secret)
	sig2 := SignPayload([]byte(`body2`), secret)
	if sig1 == sig2 {
		t.Error("different bodies produced same signature")
	}
}

// --- Integration test: Manager + Dispatcher + test server ---

func TestIntegrationEmitAndDeliver(t *testing.T) {
	var received []Payload
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p Payload
		json.Unmarshal(body, &p)
		mu.Lock()
		received = append(received, p)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(WithMaxRetries(0), WithBaseDelay(10*time.Millisecond))
	d.Start()
	m := NewManager(d, 100)

	// Register hooks for different events
	m.Register(server.URL, []EventType{EventTaskCreated, EventTaskCompleted}, "secret")
	m.Register(server.URL, []EventType{EventNodeJoined}, "secret")

	// Emit events
	n1 := m.Emit(EventTaskCreated, map[string]string{"task_id": "t1"})
	n2 := m.Emit(EventTaskCompleted, map[string]string{"task_id": "t2"})
	n3 := m.Emit(EventNodeJoined, map[string]string{"node_id": "n1"})
	n4 := m.Emit(EventNodeLeft, nil) // no hooks for this

	// Wait for async delivery
	time.Sleep(200 * time.Millisecond)
	d.Stop()

	if n1 != 1 {
		t.Errorf("Emit(task_created) triggered %d hooks, want 1", n1)
	}
	if n2 != 1 {
		t.Errorf("Emit(task_completed) triggered %d hooks, want 1", n2)
	}
	if n3 != 1 {
		t.Errorf("Emit(node_joined) triggered %d hooks, want 1", n3)
	}
	if n4 != 0 {
		t.Errorf("Emit(node_left) triggered %d hooks, want 0", n4)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("server received %d payloads, want 3", len(received))
	}
	// Verify event types
	types := make(map[EventType]bool)
	for _, p := range received {
		types[p.EventType] = true
	}
	if !types[EventTaskCreated] || !types[EventTaskCompleted] || !types[EventNodeJoined] {
		t.Errorf("missing expected event types in received payloads: %v", types)
	}
}

func TestIntegrationDeliveriesRecorded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewDispatcher(WithMaxRetries(0), WithBaseDelay(10*time.Millisecond))
	d.Start()
	m := NewManager(d, 100)

	h, _ := m.Register(server.URL, []EventType{EventTaskCreated}, "secret")

	m.Emit(EventTaskCreated, map[string]string{"task_id": "t1"})
	time.Sleep(200 * time.Millisecond)
	d.Stop()

	deliveries := m.GetDeliveries(h.ID)
	if len(deliveries) != 1 {
		t.Fatalf("GetDeliveries returned %d, want 1", len(deliveries))
	}
	if deliveries[0].Status != DeliverySuccess {
		t.Errorf("delivery status = %q, want %q", deliveries[0].Status, DeliverySuccess)
	}
}

// --- Helper ---

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	d := NewDispatcher(WithMaxRetries(0))
	return NewManager(d, 100)
}
