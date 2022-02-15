package timer

import (
	"fennel/lib/ftypes"
	"fmt"
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

func Start(tierID ftypes.TierID, funcName string) *prometheus.Timer {
	return prometheus.NewTimer(fnDuration.WithLabelValues(fmt.Sprintf("%d", tierID), funcName))
}
