package redis

import (
	"context"
	"fmt"
	"time"

	"fennel/lib/cache"

	"github.com/go-redis/redis/v8"
)

type Cache struct {
	client Client
}

func (c Cache) MGet(ctx context.Context, k ...string) ([]interface{}, error) {
	return c.client.MGet(ctx, k...)
}

func (c Cache) MSet(ctx context.Context, ks []string, vs []interface{}, ttls []time.Duration) error {
	return c.client.MSet(ctx, ks, vs, ttls)
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

	// NOTE: it is possible that the txnLogic successfully executes for a subset of keys
	// and fails for the rest e.g.
	//  i) a certain subset of keys are retried more than the rest (in which case increasing `r` should help)
	//  ii) txnLogic could fail specifically for certain keys
	//  iii) redis level errors for certain keys/slots
	//
	// Upon encountering first error, the retries are aborted and cache entries for the keys are invalidated
	for rctr := 0; rctr < r; rctr++ {
		done := 0
		invalidate := false
		for slot, keys := range slotToKeys {
			if slotToResult[slot] {
				done++
				continue
			}
			err := rc.Watch(ctx, func(t *redis.Tx) error {
				return txnLogic(NewCache(Client{client: t, Scope: c.client.Scope, conf: c.client.conf}), keys)
			}, c.client.mTieredKey(keys)...)

			// txnLogic was executed successfully
			if err == nil {
				slotToResult[slot] = true
				// txn for the keys in this slot were successfully committed
				done++
				continue
			}

			// there was an error other than Txn failure due to key conflict; non-retriable
			if err != redis.TxFailedErr {
				invalidate = true
				break
			}
		}
		if done == len(slotToKeys) {
			// watch for every slot succeeded
			return nil
		}
		if invalidate {
			break
		}
	}

	// In case of a failure or exhausting retries, delete all the cache entries
	for _, k := range ks {
		if err := c.Delete(ctx, k); err != nil {
			return err
		}
	}

	// TODO: Report the number of retries for monitoring and insights
	return fmt.Errorf("logic could not be committed after %d retries. Keys: %+v", r, ks)
}
