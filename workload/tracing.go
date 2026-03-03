package workload

import (
	"context"
	"errors"
	"fmt"
	"strings"

	gotel "github.com/couchbase/gocb-opentelemetry"
	"github.com/couchbaselabs/spectroperf/configuration"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func SetupOTelSDK(
	ctx context.Context,
	logger *zap.Logger,
	config *configuration.Config,
) (shutdown func(context.Context) error, tracer *gotel.OpenTelemetryRequestTracer, err error) {
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
	if config.EnableTracing {
		var te *otlptrace.Exporter

		if config.OtelExporterHeaders != "" {
			headers := map[string]string{}
			for _, h := range strings.Split(config.OtelExporterHeaders, ",") {
				splitHeader := strings.Split(h, "=")
				if len(splitHeader) != 2 {
					return nil, nil, fmt.Errorf("invalid otel-exporter-headers format: %s", h)
				}

				headers[splitHeader[0]] = splitHeader[1]
			}

			te, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(config.OtlpEndpoint), otlptracehttp.WithHeaders(headers))
		} else {
			te, err = otlptracehttp.New(ctx, otlptracehttp.WithInsecure(), otlptracehttp.WithEndpoint(config.OtlpEndpoint))
		}

		if err != nil {
			return nil, nil, handleErr(err)
		}

		res, err := resource.New(ctx,
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
		if config.OtlpEndpoint != configuration.DefaultOtlpEndpoint || len(config.OtelExporterHeaders) > 0 {
			logger.Warn("tracing is disabled but OTEL exporter configuration is set; these values will be ignored",
				zap.String("otlpEndpoint", config.OtlpEndpoint),
				zap.Any("otelExporterHeaders", config.OtelExporterHeaders),
			)
		}

		tp = tracenoop.NewTracerProvider()
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return shutdown, gotel.NewOpenTelemetryRequestTracer(tp), nil
}
