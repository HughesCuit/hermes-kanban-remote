package telemetry

import (
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// responseRecorder captures the status code for metrics.
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

// Middleware returns a chi-compatible middleware that creates spans and records
// request duration metrics for every HTTP request.
func Middleware(m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Start span
			ctx, span := Tracer("hac.api").Start(r.Context(),
				r.Method+" "+r.URL.Path,
				trace.WithAttributes(
					semconv.HTTPMethod(r.Method),
					semconv.HTTPRoute(r.URL.Path),
					attribute.String("http.url", r.URL.String()),
					semconv.ServerAddress(r.Host),
				),
			)
			defer span.End()

			// Rec with trace context
			r = r.WithContext(ctx)

			// Record response
			rec := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rec, r)

			duration := time.Since(start).Seconds()

			// Span attributes
			span.SetAttributes(
				semconv.HTTPResponseStatusCode(rec.statusCode),
				attribute.Float64("http.duration", duration),
			)

			if rec.statusCode >= 500 {
				span.SetAttributes(attribute.Bool("error", true))
			}

			// Metrics
			if m != nil {
				ctx := r.Context()
				m.HTTPRequestDuration.Record(ctx, duration,
					metric.WithAttributes(
						semconv.HTTPMethod(r.Method),
						semconv.HTTPRoute(r.URL.Path),
						attribute.String("status_code", strconv.Itoa(rec.statusCode)),
					),
				)
			}
		})
	}
}