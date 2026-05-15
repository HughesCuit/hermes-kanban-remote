package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerReturnsIndexHTML(t *testing.T) {
	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Hermes Agent Cluster") {
		t.Error("response body does not contain dashboard title")
	}
	if !strings.Contains(body, "<!DOCTYPE html>") {
		t.Error("response body does not appear to be HTML")
	}
}

func TestHandlerReturnsCorrectContentType(t *testing.T) {
	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("expected Content-Type to contain text/html, got %s", ct)
	}
}

func TestHandlerReturns404ForMissingFile(t *testing.T) {
	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/nonexistent.js", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandlerServesCSS(t *testing.T) {
	// The HTML file has inline CSS, but verify the handler structure works
	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	if !strings.Contains(body, "--bg-primary") {
		t.Error("response does not contain CSS custom properties")
	}
}

func TestHandlerContainsAPIEndpoints(t *testing.T) {
	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	// Verify the dashboard references the API endpoints
	endpoints := []string{"/api/v1", "/status", "/nodes", "/workflow/graph", "/recovery/stats"}
	for _, ep := range endpoints {
		if !strings.Contains(body, ep) {
			t.Errorf("dashboard HTML missing API endpoint reference: %s", ep)
		}
	}
}

func TestHandlerContainsAutoRefresh(t *testing.T) {
	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	if !strings.Contains(body, "setInterval") {
		t.Error("dashboard HTML missing auto-refresh (setInterval)")
	}
}
