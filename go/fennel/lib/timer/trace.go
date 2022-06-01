package timer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
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

type traceEvent struct {
	event   string
	elapsed time.Duration
}

type trace struct {
	lock   sync.Mutex
	start  time.Time
	xrayId string
	events []traceEvent
	spans []oteltrace.Span
}

func (t *trace) recordStart(key string, ts time.Time, span oteltrace.Span) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.events = append(t.events, traceEvent{
		event:   key,
		elapsed: ts.Sub(t.start),
	})
	t.spans = append(t.spans, span)
}

func (t *trace) recordStop(key string, ts time.Time) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.events = append(t.events, traceEvent{
		event:   key,
		elapsed: ts.Sub(t.start),
	})
}

func WithTracing(ctx context.Context, xrayId string) context.Context {
	return context.WithValue(ctx, traceKey{}, &trace{
		lock:  sync.Mutex{},
		start: time.Now(),
		xrayId: xrayId,
	})
}

func LogTracingInfo(ctx context.Context, log *zap.Logger, spanExporter sdktrace.SpanExporter) error {
	ctxval := ctx.Value(traceKey{})
	if ctxval == nil {
		return nil
	}
	trace, ok := ctxval.(*trace)
	if !ok {
		return fmt.Errorf("expected trace but got: %v", ctxval)
	}
	sb := strings.Builder{}
	sb.WriteString("====Trace====\n")
	sort.Slice(trace.events, func(i, j int) bool {
		return trace.events[i].elapsed < trace.events[j].elapsed
	})
	for _, e := range trace.events {
		sb.WriteString(fmt.Sprintf("\t%5dms: %s\n", e.elapsed.Milliseconds(), e.event))
	}
	sb.WriteString("==== X-Ray Trace =====\n")
	sb.WriteString(fmt.Sprintf("x-ray traceid: %s\n", trace.xrayId))
	log.Info(sb.String())

	// cast span into ReadOnlySpan which is required for exporting the spans
	spans := make([]sdktrace.ReadOnlySpan, len(trace.spans))
	for i, s := range trace.spans {
		log.Info(fmt.Sprintf("about to type cast span: %s", s.SpanContext().SpanID()))
		spans[i] = s.(sdktrace.ReadOnlySpan)
		log.Info(fmt.Sprintf("type casting span: %s\n", spans[i].SpanContext().SpanID()))
	}
	return spanExporter.ExportSpans(ctx, spans)
}

func InitProvider(endpoint string) (sdktrace.SpanExporter, error) {
	ctx := context.Background()

	// create and start new OTLP trace exporter
	traceExporter, err := otlptracegrpc.New(
		ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint(endpoint))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter, err: %v", err)
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

	// sample only 1% of the traces at the root node. By default, if the parent is sampled, the children nodes
	// are sampled as well (local or remote trace)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(/*root*/ sdktrace.TraceIDRatioBased(0.01))),
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithIDGenerator(idg))

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(xray.Propagator{})
	// TODO(mohit): Consider returning the shutdown callback
	return traceExporter, nil
}

var _ sdktrace.SpanExporter = (*NoopExporter)(nil)

// NewNoopExporter returns a new no-op exporter.
func NewNoopExporter() *NoopExporter {
	return new(NoopExporter)
}

// NoopExporter is an exporter that drops all received spans and performs no
// action.
type NoopExporter struct{}

// ExportSpans handles export of spans by dropping them.
func (nsb *NoopExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }

// Shutdown stops the exporter by doing nothing.
func (nsb *NoopExporter) Shutdown(context.Context) error { return nil }

func GetXrayTraceID(span oteltrace.Span) string {
	xrayTraceID := span.SpanContext().TraceID().String()
	result := fmt.Sprintf("1-%s-%s", xrayTraceID[0:8], xrayTraceID[8:])
	return result
}
