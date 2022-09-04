package ristretto

import (
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var ristrettoStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "ristretto_stats",
	Help: "Metrics for ristretto caches",
}, []string{"name", "metric"})

func ReportPeriodically(name string, c *ristretto.Cache, period time.Duration) {
	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for ; true; <-ticker.C {
			ristrettoStats.WithLabelValues(name, "hits").Set(float64(c.Metrics.Hits()))
			ristrettoStats.WithLabelValues(name, "misses").Set(float64(c.Metrics.Misses()))
			ristrettoStats.WithLabelValues(name, "ratio").Set(c.Metrics.Ratio())

			ristrettoStats.WithLabelValues(name, "sets_dropped").Set(float64(c.Metrics.SetsDropped()))
			ristrettoStats.WithLabelValues(name, "sets_rejected").Set(float64(c.Metrics.SetsRejected()))
			ristrettoStats.WithLabelValues(name, "gets_dropped").Set(float64(c.Metrics.GetsDropped()))
			ristrettoStats.WithLabelValues(name, "gets_kept").Set(float64(c.Metrics.GetsKept()))

			ristrettoStats.WithLabelValues(name, "cost_added").Set(float64(c.Metrics.CostAdded()))
			ristrettoStats.WithLabelValues(name, "cost_evicted").Set(float64(c.Metrics.CostEvicted()))
			ristrettoStats.WithLabelValues(name, "size").Set(float64(c.Metrics.CostAdded() - c.Metrics.CostEvicted()))
		}
	}()
}
