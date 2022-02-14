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
		0.0:  5,
		0.50: 5,
		0.75: 5,
		0.90: 5,
		0.95: 5,
		0.99: 5,
		1.0:  5,
	},
}, []string{"tier_id", "function_name"})

func Start(tierID ftypes.TierID, funcName string) *prometheus.Timer {
	return prometheus.NewTimer(fnDuration.WithLabelValues(fmt.Sprintf("%d", tierID), funcName))
}
