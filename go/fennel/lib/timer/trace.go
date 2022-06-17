package timer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type TracerArgs struct {
	OtlpEndpoint string `arg:"--otlp-endpoint,env:OTLP_ENDPOINT" default:""`
}

type traceKey struct{}

type trace struct {
	start  time.Time
	xrayId string
}

func WithTracing(ctx context.Context, xrayId string) context.Context {
	return context.WithValue(ctx, traceKey{}, &trace{
		start: time.Now(),
		xrayId: xrayId,
	})
}

func LogTracingInfo(ctx context.Context, log *zap.Logger) error {
	ctxval := ctx.Value(traceKey{})
	if ctxval == nil {
		return nil
	}
	trace, ok := ctxval.(*trace)
	if !ok {
		return fmt.Errorf("expected trace but got: %v", ctxval)
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("x-ray traceid: %s\n", trace.xrayId))
	log.Info(sb.String())
	return nil
}

func InitProvider(endpoint string) error {
	ctx := context.Background()

	// create and start new OTLP trace exporter
	traceExporter, err := otlptracegrpc.New(
		ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint(endpoint))
	if err != nil {
		return fmt.Errorf("failed to create trace exporter, err: %v", err)
	}

	idg := xray.NewIDGenerator()

	// TODO(mohit): Currently fails with a permission but should probably add this back
	// See: https://github.com/open-telemetry/opentelemetry-go-contrib/issues/1856
	//
	// Also see on how to grant service account permission to have access:
	// https://github.com/awsdocs/amazon-eks-user-guide/blob/master/doc_source/iam-roles-for-service-accounts-technical-overview.md
	//
	// Without this information, it will be difficult to map a trace to the origin pod
	//
	//eksResourceDetector := eks.NewResourceDetector()
	//resource, err := eksResourceDetector.Detect(ctx)
	//if err != nil {
	//	return fmt.Errorf("failed to detect eks resource, err: %v", err)
	//}

	tp := sdktrace.NewTracerProvider(
		// Ideally we should be sampling the traces (say at 1%) of the traces at the root node.
		// e.g. sdktrace.ParentBased(/*root*/ sdktrace.TraceIDRatioBased(0.01))
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		// By default, trace exporter exports 512 spans while maintaining a local queue of size `2048`
		//
		// Increase the queue size so that traces are dropped locally.
		//
		// Increase the batch size sent so that the queue is also emptied at a larger rate and can ingest more spans.
		//
		// Note: The otel collector (running in the same cluster) which ingests the traces exported from each service,
		// batches and exports to xray, batches the traces in sizes of `8192` or exports every 10s (regardless of size).
		sdktrace.WithBatcher(traceExporter, sdktrace.WithMaxQueueSize(20480), sdktrace.WithMaxExportBatchSize(2048)),
		sdktrace.WithIDGenerator(idg))

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(xray.Propagator{})
	// TODO(mohit): Consider returning the shutdown callback
	return nil
}

func GetXrayTraceID(span oteltrace.Span) string {
	xrayTraceID := span.SpanContext().TraceID().String()
	result := fmt.Sprintf("1-%s-%s", xrayTraceID[0:8], xrayTraceID[8:])
	return result
}
