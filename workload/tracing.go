package workload

import (
	"context"
	"errors"
	gotel "github.com/couchbase/gocb-opentelemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

var (
	Tracer = otel.Tracer("spectroperf")
)

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func SetupOTelSDK(ctx context.Context, endpoint string, enableTracing bool, honeycombKey string) (shutdown func(context.Context) error, tracer *gotel.OpenTelemetryRequestTracer, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) error {
		return errors.Join(inErr, shutdown(ctx))
	}

	var tp trace.TracerProvider
	if enableTracing {
		var te *otlptrace.Exporter

		if honeycombKey != "" {
			headers := map[string]string{}
			headers["x-honeycomb-team"] = honeycombKey
			te, err = otlptracehttp.New(context.Background(), otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithHeaders(headers))
		} else {
			te, err = otlptracehttp.New(context.Background(), otlptracehttp.WithInsecure(), otlptracehttp.WithEndpoint(endpoint))
		}

		if err != nil {
			return nil, nil, handleErr(err)
		}

		res, err := resource.New(context.Background(),
			resource.WithFromEnv(),
			resource.WithProcess(),
			resource.WithTelemetrySDK(),
			resource.WithHost(),
			resource.WithAttributes(
				// the service name used to display traces in backends
				semconv.ServiceNameKey.String("couchbase-spectroperf"),
			),
		)
		if err != nil {
			if res == nil {
				return nil, nil, handleErr(err)
			}
		}

		sdkTp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(te),
			sdktrace.WithResource(res),
		)

		shutdownFuncs = append(shutdownFuncs, sdkTp.Shutdown)
		tp = sdkTp
	} else {
		tp = tracenoop.NewTracerProvider()
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return shutdown, gotel.NewOpenTelemetryRequestTracer(tp), nil
}
