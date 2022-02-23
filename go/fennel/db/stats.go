package db

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var conn_stats = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "conn",
	Help: "Stats about number of db connections",
	// Track quantiles within small error
	Objectives: map[float64]float64{
		0.25: 0.05,
		0.50: 0.05,
		0.75: 0.05,
		0.90: 0.05,
		0.95: 0.02,
		0.99: 0.01,
	},
}, []string{"metric"})

var wait_stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "conn_wait",
	Help: "Stats about waiting on db connections",
}, []string{"metric"})

var close_stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "conn_closed",
	Help: "Stats about closed db connections",
}, []string{"metric"})

func RecordConnectionStats(db *sqlx.DB, period time.Duration) {
	stats := db.Stats()

	conn_stats.WithLabelValues("num_open").Observe(float64(stats.OpenConnections))
	conn_stats.WithLabelValues("num_in_use").Observe(float64(stats.InUse))
	conn_stats.WithLabelValues("num_idle").Observe(float64(stats.Idle))

	// Stats about waiting on connections and closed connections are exported
	// as counters from the sqlx library itself. We simply record these
	// as gauges and can downstream analyze deltas to see rate of change.
	wait_stats.WithLabelValues("duration_ms").Set(float64(stats.WaitDuration.Milliseconds()))
	wait_stats.WithLabelValues("count").Set(float64(stats.WaitCount))

	close_stats.WithLabelValues("max_idle_closed").Set(float64(stats.MaxIdleClosed))
	close_stats.WithLabelValues("max_idle_time_closed").Set(float64(stats.MaxIdleTimeClosed))
	close_stats.WithLabelValues("max_lifetime_closed").Set(float64(stats.MaxLifetimeClosed))
}
