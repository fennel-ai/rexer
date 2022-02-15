package redis

import (
	"context"
	"fennel/lib/timer"
	"fennel/resource"
	"time"
)

func (c Client) Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error {
	defer timer.Start(c.TierID(), "redis.set").ObserveDuration()
	k = c.tieredKey(k)
	return c.client.Set(ctx, k, v, ttl).Err()
}

func (c Client) Del(ctx context.Context, k ...string) error {
	defer timer.Start(c.TierID(), "redis.del").ObserveDuration()
	k = c.mTieredKey(k)
	return c.client.Del(ctx, k...).Err()
}

func (c Client) Get(ctx context.Context, k string) (interface{}, error) {
	defer timer.Start(c.TierID(), "redis.get").ObserveDuration()
	k = c.tieredKey(k)
	return c.client.Get(ctx, k).Result()
}

func (c Client) MGet(ctx context.Context, ks ...string) ([]interface{}, error) {
	defer timer.Start(c.TierID(), "redis.mget").ObserveDuration()
	// this check is to handle a bug, likely related to https://github.com/redis/node-redis/issues/125
	if len(ks) == 0 {
		return []interface{}{}, nil
	}
	ks = c.mTieredKey(ks)
	return c.client.MGet(ctx, ks...).Result()
}

func (c Client) MSet(ctx context.Context, values map[string]interface{}) error {
	defer timer.Start(c.TierID(), "redis.mset").ObserveDuration()
	vals := make([]interface{}, 2*len(values))
	i := 0
	for k, v := range values {
		vals[i] = c.tieredKey(k)
		vals[i+1] = v
		i += 2
	}
	return c.client.MSet(ctx, vals...).Err()
}

func (c Client) tieredKey(k string) string {
	return resource.TieredName(c.tierID, k)
}

func (c Client) mTieredKey(ks []string) []string {
	ret := make([]string, len(ks))
	for i, k := range ks {
		ret[i] = resource.TieredName(c.tierID, k)
	}
	return ret
}
