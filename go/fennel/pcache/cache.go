package pcache

import (
	"time"

	"github.com/dgraph-io/ristretto"
)

type PCache struct {
	Cache *ristretto.Cache
}

// NewPCache creates a new instance of PCache
// https://pkg.go.dev/github.com/dgraph-io/ristretto#Config
func NewPCache(maxCost int64, averageItemCost int64) (PCache, error) {
	expectedMaxItems := maxCost / averageItemCost
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10 * expectedMaxItems,
		MaxCost:     maxCost,
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		return PCache{}, err
	}
	return PCache{
		Cache: cache,
	}, nil
}

func (pc *PCache) Set(key, value interface{}) bool {
	return pc.Cache.Set(key, value, 0)
}

func (pc *PCache) SetWithTTL(key, value interface{}, ttl time.Duration) bool {
	return pc.Cache.SetWithTTL(key, value, 0, ttl)
}

func (pc *PCache) Get(key interface{}) (interface{}, bool) {
	return pc.Cache.Get(key)
}
