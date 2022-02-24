package redis

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var connStats = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "redis_conn_state",
	Help: "Stats about current redis connection pool",
	Objectives: map[float64]float64{
		0.25: 0.05,
		0.50: 0.05,
		0.75: 0.05,
		0.90: 0.05,
		0.95: 0.02,
		0.99: 0.01,
	},
}, []string{"metric"})

var connGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "redis_conn_counters",
	Help: "Lifetime counters about redis connections of the service",
}, []string{"metric"})

func RecordConnectionStats(name string, c Client) {
	stats := c.client.PoolStats()

	connGauge.WithLabelValues(fmt.Sprintf("%s:hits", name)).Set(float64(stats.Hits))
	connGauge.WithLabelValues(fmt.Sprintf("%s:misses", name)).Set(float64(stats.Misses))
	connGauge.WithLabelValues(fmt.Sprintf("%s:timeouts", name)).Set(float64(stats.Timeouts))
	connGauge.WithLabelValues(fmt.Sprintf("%s:stale_conns_removed", name)).Set(float64(stats.StaleConns))

	connStats.WithLabelValues(fmt.Sprintf("%s:total_conns", name)).Observe(float64(stats.TotalConns))
	connStats.WithLabelValues(fmt.Sprintf("%s:idle_conns", name)).Observe(float64(stats.IdleConns))
}
