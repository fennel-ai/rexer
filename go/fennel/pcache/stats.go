package pcache

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var cacheStatsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "pcache_ratio",
	Help: "Lifetime hits/misses of process-level cache",
}, []string{"metric"})

func RecordStats(name string, p PCache) {
	cacheStatsGauge.WithLabelValues(fmt.Sprintf("%s:hits", name)).Set(float64(p.Cache.Metrics.Hits()))
	cacheStatsGauge.WithLabelValues(fmt.Sprintf("%s:misses", name)).Set(float64(p.Cache.Metrics.Misses()))
	cacheStatsGauge.WithLabelValues(fmt.Sprintf("%s:ratio", name)).Set(p.Cache.Metrics.Ratio())

	cacheStatsGauge.WithLabelValues(fmt.Sprintf("%s:sets_dropped", name)).Set(float64(p.Cache.Metrics.SetsDropped()))
	cacheStatsGauge.WithLabelValues(fmt.Sprintf("%s:sets_rejected", name)).Set(float64(p.Cache.Metrics.SetsRejected()))
	cacheStatsGauge.WithLabelValues(fmt.Sprintf("%s:gets_dropped", name)).Set(float64(p.Cache.Metrics.GetsDropped()))

	// Calculated stats
	cacheStatsGauge.WithLabelValues(fmt.Sprintf("%s:sets_fails", name)).Set(float64(p.Cache.Metrics.SetsDropped()) + float64(p.Cache.Metrics.SetsRejected()))
	cacheStatsGauge.WithLabelValues(fmt.Sprintf("%s:actual_hitratio", name)).Set(float64(p.Cache.Metrics.Hits())/float64(p.Cache.Metrics.Hits()) + float64(p.Cache.Metrics.Misses()) + float64(p.Cache.Metrics.GetsDropped()))
}
