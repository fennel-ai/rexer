package counter

import (
	"context"
	"encoding/binary"
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
	rkeys := f.redisKeys(f.name, buckets)
	return readFromRedis(ctx, tier, rkeys, default_)
}

func (f FlatRedisStorage) Set(ctx context.Context, tier tier.Tier, buckets []Bucket) error {
	rkeys := f.redisKeys(f.name, buckets)
	vals := make([]value.Value, len(buckets))
	for i := range buckets {
		vals[i] = buckets[i].Value
	}
	tier.Logger.Info("Updating redis keys for aggregate", zap.String("aggregate", string(f.name)), zap.Int("num_keys", len(rkeys)))
	return setInRedis(ctx, tier, rkeys, vals, make([]time.Duration, len(rkeys)))
}

func (f FlatRedisStorage) redisKeys(name ftypes.AggName, buckets []Bucket) []string {
	ret := make([]string, len(buckets))
	for i, b := range buckets {
		ret[i] = fmt.Sprintf("agg:%s:%s:%d:%d:%d", name, b.Key, b.Window, b.Width, b.Index)
	}
	return ret
}

var _ BucketStore = FlatRedisStorage{}

/*
	twoLevelRedisStore groups all keys that fall within a period and stores them as value.Dict in a single redis key
	within that dictionary, the key is derived from the index of the key within that group
	as a result, it can only store those buckets where Window x Width is a divisor of period (else it throws error)

	As an example, if we set period to be 1 day and send it keys with window of Hour and Width of 2, we create
	one redis key for each day. Within that key, we have a dictionary and that dictionary stores upto 12 keys,
	where each key denotes a 2hr window within that day (and is called as slot in the code). This is sparse storage
	so keys for which there is no data aren't stored at all. It further reduces storage by using byte strings for
	indices which can become larger so that we reduce the overhead of per slot key as much as we can.

	The system can simultaneously handle buckets of multiple Window/Width as long as period is an even multiple
	of their total size (window x width). Prefix of all keys is agg_l2 which can help identify raw redis keys which
	come from this storage policy.

*/
type twoLevelRedisStore struct {
	name      ftypes.AggName
	period    uint64
	retention uint64
}

func NewTwoLevelStorage(name ftypes.AggName, period, retention uint64) BucketStore {
	return twoLevelRedisStore{
		name:      name,
		period:    period,
		retention: retention,
	}
}

type slot struct {
	g      group
	window ftypes.Window
	width  uint64
	idx    int
	val    value.Value
}

type group struct {
	key string
	id  uint64
}

func (t twoLevelRedisStore) Get(ctx context.Context, tier tier.Tier, buckets []Bucket, default_ value.Value) ([]value.Value, error) {
	seen := make(map[group]int)
	slots := make([]slot, len(buckets))
	rkeys := make([]string, 0)
	for i := range buckets {
		s, err := t.toSlot(&buckets[i])
		if err != nil {
			return nil, err
		}
		slots[i] = s
		g := s.g
		if _, ok := seen[g]; !ok {
			seen[g] = len(seen)
			rkeys = append(rkeys, t.redisKey(g))
		}
	}
	groupVals, err := readFromRedis(ctx, tier, rkeys, value.Dict{})
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(buckets))
	for i, s := range slots {
		g := s.g
		ptr := seen[g]
		dict, ok := groupVals[ptr].(value.Dict)
		if !ok {
			return nil, fmt.Errorf("could not read data: expected dict but found: %v\n", groupVals[ptr])
		}
		idxStr := t.slotKey(s)
		ret[i], ok = dict[idxStr]
		if !ok {
			ret[i] = default_
		}
	}
	return ret, nil
}

func (t twoLevelRedisStore) slotKey(s slot) string {
	// since minute is the more common type which also produces a lot of keys,
	// we use this window by default to save couple more bytes per slot key
	if s.window == ftypes.Window_MINUTE {
		return fmt.Sprintf("%v%v", toBuf(s.width), toBuf(uint64(s.idx)))
	} else {
		return fmt.Sprintf("%d:%v%v", s.window, toBuf(s.width), toBuf(uint64(s.idx)))
	}
}

func toBuf(n uint64) []byte {
	buf := make([]byte, 8)
	k := binary.PutUvarint(buf, n)
	return buf[:k]
}

func (t twoLevelRedisStore) redisKey(g group) string {
	return fmt.Sprintf("agg_l2:%s:%s:%v%v", t.name, g.key, toBuf(t.period), toBuf(g.id))
}

func (t twoLevelRedisStore) toSlot(b *Bucket) (slot, error) {
	d := toDuration(b.Window) * b.Width
	if t.period%d != 0 {
		return slot{}, fmt.Errorf("can only store buckets with width that can fully fit in period of: '%d'sec", t.period)
	}
	start_ts := d * b.Index
	gap := start_ts % t.period
	return slot{
		g: group{
			key: b.Key,
			id:  start_ts / t.period,
		},
		window: b.Window,
		width:  b.Width,
		idx:    int(gap / d),
		val:    b.Value,
	}, nil
}

func toDuration(w ftypes.Window) uint64 {
	switch w {
	case ftypes.Window_DAY:
		return 24 * 3600
	case ftypes.Window_HOUR:
		return 3600
	case ftypes.Window_MINUTE:
		return 60
	}
	return 0
}

func (t twoLevelRedisStore) Set(ctx context.Context, tier tier.Tier, buckets []Bucket) error {
	seen := make(map[group]int)
	slots := make([]slot, len(buckets))
	rkeys := make([]string, 0)
	for i := range buckets {
		s, err := t.toSlot(&buckets[i])
		if err != nil {
			return err
		}
		slots[i] = s
		g := s.g
		if _, ok := seen[g]; !ok {
			seen[g] = len(seen)
			rkeys = append(rkeys, t.redisKey(g))
		}
	}
	groupVals, err := readFromRedis(ctx, tier, rkeys, value.Dict{})
	if err != nil {
		return err
	}
	for _, s := range slots {
		g := s.g
		ptr := seen[g]
		dict, ok := groupVals[ptr].(value.Dict)
		if !ok {
			return fmt.Errorf("could not read data: expected dict but found: %v\n", groupVals[ptr])
		}
		idxStr := t.slotKey(s)
		dict[idxStr] = s.val
		groupVals[ptr] = dict
	}
	// we set each key with a ttl of retention seconds
	ttls := make([]time.Duration, len(rkeys))
	for i := range ttls {
		ttls[i] = time.Second * time.Duration(t.retention)
	}
	tier.Logger.Info("Updating redis keys for aggregate", zap.String("aggregate", string(t.name)), zap.Int("num_keys", len(rkeys)))
	return setInRedis(ctx, tier, rkeys, groupVals, ttls)
}

var _ BucketStore = twoLevelRedisStore{}

func Update(ctx context.Context, tier tier.Tier, buckets []Bucket, histogram Histogram) error {
	cur, err := histogram.Get(ctx, tier, buckets, histogram.Zero())
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
	return histogram.Set(ctx, tier, buckets)
}

//==========================================================
// Private helpers for talking to redis
//==========================================================

func readFromRedis(ctx context.Context, tier tier.Tier, rkeys []string, default_ value.Value) ([]value.Value, error) {
	res, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(rkeys))
	for i, v := range res {
		if ret[i], err = interpretRedisResponse(v, default_.Clone()); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func interpretRedisResponse(v interface{}, default_ value.Value) (value.Value, error) {
	switch t := v.(type) {
	case nil:
		return default_, nil
	case error:
		if t != redis.Nil {
			return nil, t
		} else {
			return default_, nil
		}
	case string:
		return value.FromJSON([]byte(t))
	default:
		return nil, fmt.Errorf("unexpected type from redis")
	}
}

var logSizes = false

func setInRedis(ctx context.Context, tier tier.Tier, rkeys []string, values []value.Value, ttls []time.Duration) error {
	if len(rkeys) != len(values) || len(rkeys) != len(ttls) {
		return fmt.Errorf("can not set in redis: keys, values, ttls should be of equal length")
	}
	vals := make([]interface{}, len(rkeys))
	for i := range rkeys {
		s, err := value.ToJSON(values[i])
		if err != nil {
			return err
		}
		vals[i] = s
	}
	return tier.Redis.MSet(ctx, rkeys, vals, ttls)
}
