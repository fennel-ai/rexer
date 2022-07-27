package cache

import (
	"github.com/dgraph-io/ristretto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var cacheStatsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "hangar_cache_stats",
	Help: "Lifetime hits/misses of hangar cache",
}, []string{"metric"})

func reportStats(c *ristretto.Cache) {
	cacheStatsGauge.WithLabelValues("hits").Set(float64(c.Metrics.Hits()))
	cacheStatsGauge.WithLabelValues("misses").Set(float64(c.Metrics.Misses()))
	cacheStatsGauge.WithLabelValues("ratio").Set(c.Metrics.Ratio())

	cacheStatsGauge.WithLabelValues("sets_dropped").Set(float64(c.Metrics.SetsDropped()))
	cacheStatsGauge.WithLabelValues("sets_rejected").Set(float64(c.Metrics.SetsRejected()))
	cacheStatsGauge.WithLabelValues("gets_dropped").Set(float64(c.Metrics.GetsDropped()))

	// Calculated stats
	cacheStatsGauge.WithLabelValues("actual_hitratio").Set(float64(c.Metrics.Hits()) / (float64(c.Metrics.Hits()) + float64(c.Metrics.Misses()) + float64(c.Metrics.GetsDropped())))
}
