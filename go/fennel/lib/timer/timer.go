package timer

import (
	"context"
	"fmt"

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

func (t Timer) Stop() {
	t.timer.ObserveDuration()
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
