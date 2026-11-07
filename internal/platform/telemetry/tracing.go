package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// SetupTracing installs the global tracer provider and W3C propagator. When
// endpoint is empty spans are still created (so trace ids appear in logs and
// propagate to workers) but nothing is exported. The OTLP exporter also honours
// the standard OTEL_EXPORTER_OTLP_* environment variables.
func SetupTracing(ctx context.Context, serviceName, version, endpoint string, sampleRatio float64) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	res := resource.NewSchemaless(
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(version),
	)
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
	}
	if endpoint != "" {
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
		if err != nil {
			return nil, fmt.Errorf("create otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
