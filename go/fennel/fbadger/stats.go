package fbadger

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var cache_stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "badger_cache_stats",
	Help: "Stats about Badger cache",
}, []string{"metric"})

func RecordBadgerCacheStats(db DB) {
	// TODO(mohit): Remove the metrics which are not of interest for us
	// potentially: index cache metrics, block: cost_added, sets and gets dropped/rejected
	blockMetrics := db.BlockCacheMetrics()
	cache_stats.WithLabelValues("block:hits").Set(float64(blockMetrics.Hits()))
	cache_stats.WithLabelValues("block:misses").Set(float64(blockMetrics.Misses()))
	cache_stats.WithLabelValues("block:keys_added").Set(float64(blockMetrics.KeysAdded()))
	cache_stats.WithLabelValues("block:keys_updated").Set(float64(blockMetrics.KeysUpdated()))
	cache_stats.WithLabelValues("block:keys_evicted").Set(float64(blockMetrics.KeysEvicted()))
	cache_stats.WithLabelValues("block:cost_added").Set(float64(blockMetrics.CostAdded()))
	cache_stats.WithLabelValues("block:sets_dropped").Set(float64(blockMetrics.SetsDropped()))
	cache_stats.WithLabelValues("block:sets_rejected").Set(float64(blockMetrics.SetsRejected()))
	cache_stats.WithLabelValues("block:gets_dropped").Set(float64(blockMetrics.GetsDropped()))
	cache_stats.WithLabelValues("block:ratio").Set(blockMetrics.Ratio())

	indexMetrics := db.IndexCacheMetrics()
	cache_stats.WithLabelValues("index:hits").Set(float64(indexMetrics.Hits()))
	cache_stats.WithLabelValues("index:misses").Set(float64(indexMetrics.Misses()))
	cache_stats.WithLabelValues("index:keys_added").Set(float64(indexMetrics.KeysAdded()))
	cache_stats.WithLabelValues("index:keys_updated").Set(float64(indexMetrics.KeysUpdated()))
	cache_stats.WithLabelValues("index:keys_evicted").Set(float64(indexMetrics.KeysEvicted()))
	cache_stats.WithLabelValues("index:cost_added").Set(float64(indexMetrics.CostAdded()))
	cache_stats.WithLabelValues("index:sets_dropped").Set(float64(indexMetrics.SetsDropped()))
	cache_stats.WithLabelValues("index:sets_rejected").Set(float64(indexMetrics.SetsRejected()))
	cache_stats.WithLabelValues("index:gets_dropped").Set(float64(indexMetrics.GetsDropped()))
	cache_stats.WithLabelValues("index:ratio").Set(indexMetrics.Ratio())
}
