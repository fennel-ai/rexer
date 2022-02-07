package counter

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/tier"
	"fmt"
	"strconv"

	"github.com/go-redis/redis/v8"
)

func redisKeys(tier tier.Tier, name ftypes.AggName, buckets []Bucket) []string {
	ret := make([]string, len(buckets))
	for i, b := range buckets {
		ret[i] = fmt.Sprintf("counter:%d:%s:%s:%d:%d", version, name, b.Key, b.Window, b.Index)
	}
	return ret
}

func IncrementMulti(tier tier.Tier, name ftypes.AggName, buckets []Bucket) error {
	rkeys := redisKeys(tier, name, buckets)
	cur, err := GetMulti(tier, name, buckets)
	if err != nil {
		return err
	}
	vals := make([]string, 2*len(rkeys))
	for i, k := range rkeys {
		vals[2*i] = k
		vals[2*i+1] = fmt.Sprintf("%d", cur[i]+buckets[i].Count)
	}
	return tier.Redis.MSet(context.TODO(), vals).Err()
}

func Get(tier tier.Tier, name ftypes.AggName, bucket Bucket) (int64, error) {
	ret, err := GetMulti(tier, name, []Bucket{bucket})
	if err != nil {
		return 0, err
	}
	return ret[0], nil
}

func GetMulti(tier tier.Tier, name ftypes.AggName, buckets []Bucket) ([]int64, error) {
	rkeys := redisKeys(tier, name, buckets)
	res, err := tier.Redis.MGet(context.TODO(), rkeys...).Result()
	if err != nil {
		return nil, err
	}
	ret := make([]int64, len(buckets))
	for i, v := range res {
		switch t := v.(type) {
		case nil:
			ret[i] = 0
		case error:
			if t != redis.Nil {
				return nil, t
			} else {
				ret[i] = 0
			}
		case string:
			ret[i], err = strconv.ParseInt(t, 10, 64)
			if err != nil {
				return nil, err
			}
		case int64:
			ret[i] = t
		case int:
			ret[i] = int64(t)
		}
	}
	return ret, nil
}
