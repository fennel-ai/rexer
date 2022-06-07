package pcache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"

	"github.com/dgraph-io/ristretto"
)

type PCache struct {
	Cache *ristretto.Cache
}

var cacheHits = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pcache_hits_namespace",
		Help: "Number of P Cache hits per namespace.",
	},
	[]string{"namespace"},
)

var cacheMisses = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pcache_misses_namespace",
		Help: "Number of P Cache misses per namespace.",
	},
	[]string{"namespace"},
)

var cacheSets = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pcache_sets_namespace",
		Help: "Number of P Cache sets per namespace.",
	},
	[]string{"namespace"},
)

var cacheEvicts = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pcache_evicts_namespace",
		Help: "Number of P Cache evicts per namespace.",
	},
	[]string{"namespace"},
)

// NewPCache creates a new instance of PCache
// https://pkg.go.dev/github.com/dgraph-io/ristretto#Config
func NewPCache(maxCost int64, averageItemCost int64) (PCache, error) {
	expectedMaxItems := maxCost / averageItemCost
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10 * expectedMaxItems,
		MaxCost:     maxCost,
		// Ristretto recommends BufferItems as `64`, but we have noticed a large number of sets being dropped,
		// therefore we set this value as `1024`; The exact value is TBD and should be tuned using the help
		// of the metrics reported to Prometheus
		BufferItems: 1 << 10,
		Metrics:     true,
	})
	if err != nil {
		return PCache{}, err
	}
	return PCache{
		Cache: cache,
	}, nil
}

func (pc *PCache) Set(key, value interface{}, cost int64) bool {
	return pc.Cache.Set(key, value, cost)
}

func (pc *PCache) SetWithTTL(key, value interface{}, cost int64, ttl time.Duration, namespace string) bool {
	ok := pc.Cache.SetWithTTL(key, value, cost, ttl)
	if ok {
		cacheSets.WithLabelValues(namespace).Inc()
	}
	return ok
}

func (pc *PCache) Get(key interface{}, namespace string) (interface{}, bool) {
	val, ok := pc.Cache.Get(key)
	if ok {
		cacheHits.WithLabelValues(namespace).Inc()
	} else {
		cacheMisses.WithLabelValues(namespace).Inc()
	}
	return val, ok
}

func (pc *PCache) GetTTL(key interface{}) (time.Duration, bool) {
	return pc.Cache.GetTTL(key)
}
