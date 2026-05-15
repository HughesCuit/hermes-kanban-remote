package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.written {
		rw.statusCode = code
		rw.written = true
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.written = true
	}
	return rw.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// Middleware returns an HTTP middleware that records request count and duration.
// Path labels are normalized: numeric IDs and UUIDs are replaced with {id}
// to keep cardinality bounded.
func Middleware(c *Collector) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := newResponseWriter(w)

			next.ServeHTTP(rw, r)

			duration := time.Since(start).Seconds()
			status := strconv.Itoa(rw.statusCode)
			path := normalizePath(r.URL.Path)

			c.HTTPRequestsTotal.WithLabelValues(r.Method, path, status).Inc()
			c.HTTPRequestDuration.WithLabelValues(r.Method, path).Observe(duration)
		})
	}
}

// normalizePath reduces high-cardinality path labels by replacing
// dynamic segments (IDs, UUIDs) with a placeholder.
func normalizePath(path string) string {
	// Split path into segments and normalize each
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		if seg == "" {
			continue
		}
		// UUID pattern: 8-4-4-4-12 hex chars
		if isUUID(seg) {
			segments[i] = "{id}"
			continue
		}
		// Long hex strings (task IDs like task_a1b2c3d4e5f6)
		if strings.HasPrefix(seg, "task_") || strings.HasPrefix(seg, "lease_") || strings.HasPrefix(seg, "node_") {
			segments[i] = normalizeIDPrefix(seg)
			continue
		}
		// Numeric IDs
		if isNumericID(seg) {
			segments[i] = "{id}"
		}
	}
	return strings.Join(segments, "/")
}

func normalizeIDPrefix(seg string) string {
	parts := strings.SplitN(seg, "_", 2)
	if len(parts) == 2 && len(parts[1]) > 8 {
		return parts[0] + "_{id}"
	}
	return seg
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch {
		case c == '-' && (i == 8 || i == 13 || i == 18 || i == 23):
			continue
		case (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'):
			continue
		default:
			return false
		}
	}
	return true
}

func isNumericID(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
