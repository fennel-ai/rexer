package timer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/utils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var fnDuration = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "fn_duration_seconds",
	Help: "Duration of individual go functions",
	Objectives: map[float64]float64{
		0.25: 0.05,
		0.50: 0.05,
		0.75: 0.05,
		0.90: 0.05,
		0.95: 0.02,
		0.99: 0.01,
	},
}, []string{"realm_id", "function_name"})

type Timer struct {
	span       string
	realmID    ftypes.RealmID
	timer      *prometheus.Timer
	trace      *trace
	id         string
	tracerSpan oteltrace.Span
}

func joinStr(s ...string) string {
	var sb strings.Builder
	for _, str := range s {
		sb.WriteString(str)
	}
	return sb.String()
}

func (t Timer) Stop() {
	t.timer.ObserveDuration()
	if t.trace != nil {
		t.trace.recordStop(joinStr("exit:", t.id, ":", t.span), time.Now())
	}
	t.tracerSpan.End()
}

func Start(ctx context.Context, realmID ftypes.RealmID, funcName string) (context.Context, Timer) {
	tracer := otel.Tracer("fennel")
	cCtx, span := tracer.Start(ctx, funcName)
	ctxval := cCtx.Value(traceKey{})
	id := utils.RandString(6)
	var tr *trace = nil
	if ctxval != nil {
		tr = ctxval.(*trace)
		tr.recordStart(joinStr("enter:", id, ":", funcName), time.Now(), span)
	}
	return cCtx, Timer{
		span:       funcName,
		realmID:    realmID,
		timer:      prometheus.NewTimer(fnDuration.WithLabelValues(fmt.Sprintf("%d", realmID), funcName)),
		trace:      tr,
		id:         id,
		tracerSpan: span,
	}
}
