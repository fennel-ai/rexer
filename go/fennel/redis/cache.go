package redis

import (
	"context"
	"fmt"
	"time"

	"fennel/lib/cache"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Cache struct {
	client Client
}

var retry_stats = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "redis_txn_retries",
	Help: "number of redis txn retries within a Redis Watch",
})

var invalidate_failures = promauto.NewCounter(prometheus.CounterOpts{
	Name: "redis_invalidate_failures",
	Help: "Number of keys in redis cache, which could not be invalidated due to internal errors",
})

func (c Cache) MGet(ctx context.Context, k ...string) ([]interface{}, error) {
	return c.client.MGet(ctx, k...)
}

func (c Cache) MSet(ctx context.Context, ks []string, vs []interface{}, ttls []time.Duration) error {
	return c.client.MSet(ctx, ks, vs, ttls)
}

func (c Cache) MSetNoTxn(ctx context.Context, ks []string, vs []interface{}, ttls []time.Duration) error {
	return c.client.MSetNoTxn(ctx, ks, vs, ttls)
}

func (c Cache) Nil() error {
	return redis.Nil
}

var _ cache.Cache = Cache{}

func NewCache(client Client) Cache {
	return Cache{client: client}
}

func (c Cache) Get(ctx context.Context, k string) (interface{}, error) {
	return c.client.Get(ctx, k)
}

func (c Cache) Delete(ctx context.Context, k ...string) error {
	return c.client.Del(ctx, k...)
}

func (c Cache) Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error {
	return c.client.Set(ctx, k, v, ttl)
}

func (c Cache) Expire(ctx context.Context, ks []string, ttls []time.Duration) error {
	return c.client.Expire(ctx, ks, ttls)
}

func (c Cache) Persist(ctx context.Context, ks []string) error {
	return c.client.Persist(ctx, ks)
}

func (c Cache) Init() error {
	return nil
}

func (c Cache) RunAsTxn(ctx context.Context, txnLogic func(c cache.Txn, keys []string) error, ks []string, r int) error {
	if _, ok := c.client.client.(*redis.Tx); ok {
		return fmt.Errorf("can't run txns on redis.Tx object")
	}
	// `Watch` on a multi-node setup requires all the keys being watched be on the same "slot". We distribute
	// the keys here per slot and run watch on each key set of a slot
	rc := c.client.client.(*redis.ClusterClient)
	slotToKeys := make(map[int64][]string)
	for _, key := range ks {
		slot, err := rc.ClusterKeySlot(ctx, c.client.tieredKey(key)).Result()
		if err != nil {
			return err
		}
		slotToKeys[slot] = append(slotToKeys[slot], key)
	}

	slotToResult := make(map[int64]bool)
	for slot := range slotToKeys {
		slotToResult[slot] = false
	}

	rctr := 0
	// NOTE: it is possible that the txnLogic successfully executes for a subset of keys
	// and fails for the rest e.g.
	//  i) a certain subset of keys are retried more than the rest (in which case increasing `r` should help)
	//  ii) txnLogic could fail specifically for certain keys
	//  iii) redis level errors for certain keys/slots
	//
	// Upon encountering first error, the retries are aborted and cache entries for the keys are invalidated
	for ; rctr < r; rctr++ {
		errs := make(chan error, len(slotToKeys))
		for s, ks := range slotToKeys {
			// run the watch for each slot concurrently
			go func(slot int64, keys []string) {
				if slotToResult[slot] {
					errs <- nil
					return
				}
				err := rc.Watch(ctx, func(t *redis.Tx) error {
					return txnLogic(NewCache(Client{client: t, Scope: c.client.Scope, conf: c.client.conf}), keys)
				}, c.client.mTieredKey(keys)...)

				// txnLogic was executed successfully
				if err == nil {
					slotToResult[slot] = true
					// txn for the keys in this slot were successfully committed
					errs <- nil
					return
				}

				// there was an error other than Txn failure due to key conflict; non-retriable
				if err != redis.TxFailedErr {
					errs <- err
				}

				// txn logic should be retried
				errs <- nil
			}(s, ks)
		}

		done := 0
		invalidate := false
		// check if there were non-retriable error, if so, abort the watch
		for slot := range slotToKeys {
			if err := <-errs; err != nil {
				invalidate = true
				break
			}
			if slotToResult[slot] {
				done += 1
			}
		}
		if invalidate {
			break
		}
		if done == len(slotToKeys) {
			return nil
		}
	}

	// report the number of txn retry attempts
	retry_stats.Set(float64(rctr))

	// in case of a failure or exhausting retries, invalidate all the cache entries to
	// not have the cache in an inconsistent way
	err := c.Delete(ctx, ks...)
	if err != nil {
		invalidate_failures.Add(float64(len(ks)))
		return err
	}
	return nil
}
