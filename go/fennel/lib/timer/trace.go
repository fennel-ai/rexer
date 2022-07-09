package timer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"

	"github.com/go-logr/stdr"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type TracerArgs struct {
	OtlpEndpoint string `arg:"--otlp-endpoint,env:OTLP_ENDPOINT" default:""`
}

type TraceKey struct {}

type TraceVal struct {}

// PathSampler is a span sampler which samples a span if the parent context is embedded with an instance of `TraceKey`
// and non-nil value
type PathSampler struct {
	samplingRatio float32
}

func (r PathSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	c := parameters.ParentContext
	traceVal := c.Value(TraceKey{})
	psc := oteltrace.SpanContextFromContext(c)
	decision := sdktrace.Drop
	// TODO: consider making the ratio unleash configurable - this allows changing the sampling rate without
	// restarting the services
	if traceVal != nil && r.samplingRatio >= rand.Float32() {
		decision = sdktrace.RecordAndSample
	}
	return sdktrace.SamplingResult{
		Decision: decision,
		Tracestate: psc.TraceState(),
	}
}

func (r PathSampler) Description() string {
	return "PathSampler"
}

var _ sdktrace.Sampler = PathSampler{}

func createPathSampler(samplingRatio float32) PathSampler {
	return PathSampler{samplingRatio: samplingRatio}
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
		//
		// Currently only sample a span if it's parent has been sampled. Currently the parent span will be sampled
		// based on `PathSampler` and sample at 1%
		sdktrace.WithSampler(sdktrace.ParentBased(createPathSampler(0.01))),
		// By default, trace exporter exports 512 spans while maintaining a local queue of size `2048`
		//
		// Increase the queue size so that traces are dropped locally.
		//
		// Increase the batch size sent so that the queue is also emptied at a larger rate and can ingest more spans.
		//
		// Note: The otel collector (running in the same cluster) which ingests the traces exported from each service,
		// batches and exports to xray, batches the traces in sizes of `8192` or exports every 10s (regardless of size).
		//
		// Increase queuesize and the export batch size so that a lot of spans are not dropped.
		// See - https://linear.app/fennel-ai/issue/REX-1288#comment-59690710 for rough estimations with live traffic
		sdktrace.WithBatcher(traceExporter, sdktrace.WithMaxQueueSize(204800), sdktrace.WithMaxExportBatchSize(20480)),
		sdktrace.WithIDGenerator(idg))

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(xray.Propagator{})

	// set logger so that we see some log entries of the number of spans which are dropped
	// see - https://github.com/open-telemetry/opentelemetry-go/blob/main/sdk/trace/batch_span_processor.go#L268
	//
	// NOTE: This is temporary and should be eventually removed
	stdrLogger := stdr.New(log.New(os.Stderr, "", log.LstdFlags | log.Lshortfile))
	// set global verbosity of the level as 5 since Debug messages in otel collector logger are logged with V-level = 5
	// see - https://github.com/open-telemetry/opentelemetry-go/blob/575e1bb27025c73fd76f1e6b9dc2727b85867fdc/internal/global/internal_logging.go#L62
	stdr.SetVerbosity(5)
	otel.SetLogger(stdrLogger)

	// TODO(mohit): Consider returning the shutdown callback
	return nil
}

func GetXrayTraceID(span oteltrace.Span) string {
	xrayTraceID := span.SpanContext().TraceID().String()
	result := fmt.Sprintf("1-%s-%s", xrayTraceID[0:8], xrayTraceID[8:])
	return result
}
