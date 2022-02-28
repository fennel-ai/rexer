package counter

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/redis"
	"fennel/tier"
)

// global version of counter namespace - increment to invalidate all data stored in redis
const version = 1

// TODO: all keys of an aggregatore are mapped to the same slot
// this is not good and we need to find a better distribution strategy
func redisKeys(tier tier.Tier, name ftypes.AggName, buckets []Bucket) []string {
	ret := make([]string, len(buckets))
	for i, b := range buckets {
		ret[i] = fmt.Sprintf("counter:%d{%s}%s:%d:%d", version, name, b.Key, b.Window, b.Index)
	}
	return ret
}

func GetMulti(ctx context.Context, tier tier.Tier, name ftypes.AggName, buckets []Bucket, histogram Histogram) ([]value.Value, error) {
	rkeys := redisKeys(tier, name, buckets)
	res, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(buckets))
	for i, v := range res {
		switch t := v.(type) {
		case nil:
			ret[i] = histogram.Zero()
		case error:
			if t != redis.Nil {
				return nil, t
			} else {
				ret[i] = histogram.Zero()
			}
		case string:
			ret[i], err = value.FromJSON([]byte(t))
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unexpected type from redis")
		}
	}
	return ret, nil
}

func Update(ctx context.Context, tier tier.Tier, name ftypes.AggName, buckets []Bucket, histogram Histogram) error {
	rkeys := redisKeys(tier, name, buckets)
	cur, err := GetMulti(ctx, tier, name, buckets, histogram)
	if err != nil {
		return err
	}
	vals := make([]interface{}, len(cur))
	for i := range cur {
		merged, err := histogram.Merge(cur[i], buckets[i].Count)
		if err != nil {
			return err
		}
		if vals[i], err = value.ToJSON(merged); err != nil {
			return err
		}
	}
	tier.Logger.Info("Updating redis keys for aggregate", zap.String("aggregate", string(name)), zap.Int("num_keys", len(rkeys)))
	return tier.Redis.MSet(ctx, rkeys, vals, make([]time.Duration, len(rkeys)))
}
