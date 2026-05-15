package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Tracer returns a named tracer from the global provider.
// Safe to call even if no provider is set (returns noop tracer).
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}