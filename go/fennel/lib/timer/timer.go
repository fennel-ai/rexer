package timer

import (
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
}, []string{"function_name"})

type Timer struct {
	timer      *prometheus.Timer
}

func (t Timer) Stop() {
	t.timer.ObserveDuration()
}

func Start(funcName string) Timer {
	return Timer{
		timer:      prometheus.NewTimer(fnDuration.WithLabelValues(funcName)),
	}
}
