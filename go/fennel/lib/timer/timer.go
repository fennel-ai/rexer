package timer

import (
	"context"
	"fmt"
	"time"

	"fennel/lib/ftypes"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
}, []string{"tier_id", "function_name"})

type Timer struct {
	span   string
	tierID ftypes.TierID
	timer  *prometheus.Timer
	trace  *trace
}

func (t Timer) Stop() {
	t.timer.ObserveDuration()
	if t.trace != nil {
		t.trace.record(fmt.Sprintf("exit:%s", t.span), time.Now())
	}
}

func Start(ctx context.Context, tierID ftypes.TierID, funcName string) Timer {
	ctxval := ctx.Value(traceKey{})
	var tr *trace = nil
	if ctxval != nil {
		tr = ctxval.(*trace)
		tr.record(fmt.Sprintf("enter:%s", funcName), time.Now())
	}
	return Timer{
		span:   funcName,
		tierID: tierID,
		timer:  prometheus.NewTimer(fnDuration.WithLabelValues(fmt.Sprintf("%d", tierID), funcName)),
		trace:  tr,
	}
}
