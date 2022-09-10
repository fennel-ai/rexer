package timer

import (
	"context"
	"fmt"
	"time"

	"fennel/lib/ftypes"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var fnDuration = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "fn_duration_seconds",
	Help: "Duration of individual go functions",
	Objectives: map[float64]float64{
		0.25: 0.075,
		0.50: 0.05,
		0.75: 0.025,
		0.90: 0.01,
		0.95: 0.005,
		0.99: 0.001,
		0.999: 0.0001,
		0.9999: 0.00001,
	},
	// Time window now is 30 seconds wide, defaults to 10m
	//
	// NOTE: we configure this > the lowest scrape interval configured for prometheus job
	MaxAge: 30 * time.Second,
	// we slide the window every 6 (= 30 / 5 ) seconds
	AgeBuckets: 5,
}, []string{"realm_id", "function_name"})

type Timer struct {
	span       string
	realmID    ftypes.RealmID
	timer      *prometheus.Timer
	tracerSpan oteltrace.Span
}

func (t Timer) Stop() {
	t.timer.ObserveDuration()
	t.tracerSpan.End()
}

func Start(ctx context.Context, realmID ftypes.RealmID, funcName string) (context.Context, Timer) {
	tracer := otel.Tracer("fennel")
	cCtx, span := tracer.Start(ctx, funcName)
	return cCtx, Timer{
		span:       funcName,
		realmID:    realmID,
		timer:      prometheus.NewTimer(fnDuration.WithLabelValues(fmt.Sprintf("%d", realmID), funcName)),
		tracerSpan: span,
	}
}
