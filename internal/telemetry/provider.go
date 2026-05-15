package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Provider holds the initialized OTel providers and a shutdown function.
type Provider struct {
	TracerProvider *sdktrace.TracerProvider
	Shutdown       func(context.Context) error
}

// Init sets up the OTel TracerProvider based on config.
// Returns a Provider with a Shutdown func. Caller must defer provider.Shutdown(ctx).
func Init(ctx context.Context, cfg Config) (*Provider, error) {
	if !cfg.IsEnabled() {
		return &Provider{Shutdown: func(_ context.Context) error { return nil }}, nil
	}

	res, err := sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	var spanExporter sdktrace.SpanExporter
	switch cfg.Exporter {
	case "otlp":
		spanExporter, err = newOTLPExporter(ctx, cfg.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("otel otlp exporter: %w", err)
		}
	case "stdout":
		spanExporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("otel stdout exporter: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported exporter: %s", cfg.Exporter)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(spanExporter, sdktrace.WithBatchTimeout(cfg.BatchTimeout)),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.SampleRate)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Provider{
		TracerProvider: tp,
		Shutdown: func(ctx context.Context) error {
			shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			return tp.Shutdown(shutdownCtx)
		},
	}, nil
}

func newOTLPExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
}