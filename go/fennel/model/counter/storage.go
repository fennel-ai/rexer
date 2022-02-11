package counter

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/redis"
	"fennel/tier"
	"fmt"
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

func GetMulti(tier tier.Tier, name ftypes.AggName, buckets []Bucket, histogram Histogram) ([]value.Value, error) {
	rkeys := redisKeys(tier, name, buckets)
	res, err := tier.Redis.MGet(context.TODO(), rkeys...)
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
			ret[i], err = histogram.Unmarshal(t)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unexpected type from redis")
		}
	}
	return ret, nil
}

func Update(tier tier.Tier, name ftypes.AggName, buckets []Bucket, histogram Histogram) error {
	rkeys := redisKeys(tier, name, buckets)
	cur, err := GetMulti(tier, name, buckets, histogram)
	if err != nil {
		return err
	}
	vals := make(map[string]interface{}, 0)
	for i, _ := range cur {
		merged, err := histogram.Merge(cur[i], buckets[i].Count)
		if err != nil {
			return err
		}
		k := rkeys[i]
		if vals[k], err = histogram.Marshal(merged); err != nil {
			return err
		}
	}
	return tier.Redis.MSet(context.TODO(), vals)
}
