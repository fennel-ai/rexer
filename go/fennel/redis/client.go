package redis

import (
	"context"
	"fmt"
	"time"

	"fennel/lib/timer"

	"github.com/go-redis/redis/v8"
)

func (c Client) Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error {
	defer timer.Start(ctx, c.ID(), "redis.set").Stop()
	k = c.tieredKey(k)
	return c.client.Set(ctx, k, v, ttl).Err()
}

func (c Client) Del(ctx context.Context, k ...string) error {
	defer timer.Start(ctx, c.ID(), "redis.del").Stop()
	k = c.mTieredKey(k)
	return c.client.Del(ctx, k...).Err()
}

func (c Client) Get(ctx context.Context, k string) (interface{}, error) {
	defer timer.Start(ctx, c.ID(), "redis.get").Stop()
	k = c.tieredKey(k)
	return c.client.Get(ctx, k).Result()
}

// MGet takes a list of strings and returns a list of interfaces along with an error
// returned is either the correct value of key or redis.Nil
func (c Client) MGet(ctx context.Context, ks ...string) ([]interface{}, error) {
	defer timer.Start(ctx, c.ID(), "redis.mget").Stop()
	// this check is to handle a bug, likely related to https://github.com/redis/node-redis/issues/125
	if len(ks) == 0 {
		return []interface{}{}, nil
	}
	ks = c.mTieredKey(ks)
	c.client.PoolStats()
	vs, err := c.client.MGet(ctx, ks...).Result()
	for i := range vs {
		if vs[i] == nil {
			vs[i] = redis.Nil
		}
	}
	return vs, err
}

func (c Client) MSet(ctx context.Context, keys []string, values []interface{}, ttls []time.Duration) error {
	defer timer.Start(ctx, c.ID(), "redis.mset").Stop()
	if len(keys) != len(values) || len(keys) != len(ttls) {
		return fmt.Errorf("keys, values, and ttls should all be slices of the same length")
	}
	// NOTE: we are using transactioned pipeline here, which enforces cross-slot errors;
	// Looks like non-transaction pipeline doesn't enforce this kind of error, but comes
	// with weaker guarantees. Someday, we should explore this and see if non-transaction
	// pipelines make more sense for us in general
	pipe := c.client.TxPipeline()
	for i := range keys {
		pipe.Set(ctx, c.tieredKey(keys[i]), values[i], ttls[i])
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c Client) tieredKey(k string) string {
	return c.PrefixedName(k)
}

func (c Client) mTieredKey(ks []string) []string {
	ret := make([]string, len(ks))
	for i, k := range ks {
		ret[i] = c.tieredKey(k)
	}
	return ret
}
