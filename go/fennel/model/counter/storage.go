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

type FlatRedisStorage struct {
	name ftypes.AggName
}

func (f FlatRedisStorage) Get(ctx context.Context, tier tier.Tier, buckets []Bucket, default_ value.Value) ([]value.Value, error) {
	rkeys := redisKeys(tier, f.name, buckets)
	res, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(buckets))
	for i, v := range res {
		switch t := v.(type) {
		case nil:
			ret[i] = default_
		case error:
			if t != redis.Nil {
				return nil, t
			} else {
				ret[i] = default_
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

func (f FlatRedisStorage) Set(ctx context.Context, tier tier.Tier, buckets []Bucket) error {
	rkeys := redisKeys(tier, f.name, buckets)
	vals := make([]interface{}, len(buckets))
	for i := range buckets {
		s, err := value.ToJSON(buckets[i].Value)
		if err != nil {
			return err
		}
		vals[i] = s
	}
	tier.Logger.Info("Updating redis keys for aggregate", zap.String("aggregate", string(f.name)), zap.Int("num_keys", len(rkeys)))
	return tier.Redis.MSet(ctx, rkeys, vals, make([]time.Duration, len(rkeys)))
}

var _ BucketStore = FlatRedisStorage{}

func redisKeys(tier tier.Tier, name ftypes.AggName, buckets []Bucket) []string {
	ret := make([]string, len(buckets))
	for i, b := range buckets {
		ret[i] = fmt.Sprintf("agg:%s:%s:%d:%d:%d", name, b.Key, b.Window, b.Width, b.Index)
	}
	return ret
}

func GetMulti(ctx context.Context, tier tier.Tier, name ftypes.AggName, buckets []Bucket, histogram Histogram) ([]value.Value, error) {
	return FlatRedisStorage{name}.Get(ctx, tier, buckets, histogram.Zero())
}

func Update(ctx context.Context, tier tier.Tier, name ftypes.AggName, buckets []Bucket, histogram Histogram) error {
	store := FlatRedisStorage{name}
	cur, err := store.Get(ctx, tier, buckets, histogram.Zero())
	if err != nil {
		return err
	}
	for i := range cur {
		merged, err := histogram.Merge(cur[i], buckets[i].Value)
		if err != nil {
			return err
		}
		buckets[i].Value = merged
	}
	return store.Set(ctx, tier, buckets)
}
