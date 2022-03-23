package counter

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/redis"
	"fennel/tier"
)

var metrics = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "aggregate_storage_bytes",
	Help: "Distribution of storage bytes for aggregates",
	// Track quantiles within small error
	Objectives: map[float64]float64{
		0.25: 0.05,
		0.50: 0.05,
		0.75: 0.05,
		0.90: 0.05,
		0.95: 0.02,
		0.99: 0.01,
	},
}, []string{"metric"})

type FlatRedisStorage struct{}

func (f FlatRedisStorage) GetBucketStore() BucketStore {
	return f
}

func (f FlatRedisStorage) Get(
	ctx context.Context, tier tier.Tier, h Histogram, buckets []Bucket,
) ([]value.Value, error) {
	rkeys := f.redisKeys(h.Name(), buckets)
	rvals, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	vals := make([]value.Value, len(rvals))
	for i, rv := range rvals {
		vals[i], err = interpretRedisResponse(rv, h.Zero())
		if err != nil {
			return nil, err
		}
	}
	return vals, nil
}

func (f FlatRedisStorage) GetMulti(
	ctx context.Context, tier tier.Tier, buckets map[Histogram][]Bucket,
) (map[Histogram][]value.Value, error) {
	var rkeys []string
	ptr := make(map[Histogram]int, len(buckets))
	for h, buckets := range buckets {
		ptr[h] = len(rkeys)
		rkeys = append(rkeys, f.redisKeys(h.Name(), buckets)...)
	}
	rvals, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	vals := make(map[Histogram][]value.Value, len(buckets))
	for h, buckets := range buckets {
		s := ptr[h]
		for i := range buckets {
			vals[h][i], err = interpretRedisResponse(rvals[s+i], h.Zero())
		}
	}
	return vals, nil
}

func (f FlatRedisStorage) Set(ctx context.Context, tier tier.Tier, h Histogram, buckets []Bucket) error {
	rkeys := f.redisKeys(h.Name(), buckets)
	vals := make([]value.Value, len(rkeys))
	for i := range buckets {
		vals[i] = buckets[i].Value
	}
	tier.Logger.Info(
		"Updating redis keys for aggregate", zap.String("aggregate", string(h.Name())), zap.Int("num_keys", len(rkeys)),
	)
	return setInRedis(ctx, tier, rkeys, vals, make([]time.Duration, len(rkeys)))
}

func (f FlatRedisStorage) SetMulti(ctx context.Context, tier tier.Tier, buckets map[Histogram][]Bucket) error {
	var rkeys []string
	var vals []value.Value
	logKeyCount := make(map[Histogram]int, len(buckets))
	for h, buckets := range buckets {
		rkeys = append(rkeys, f.redisKeys(h.Name(), buckets)...)
		for _, b := range buckets {
			vals = append(vals, b.Value)
		}
	}
	for h := range buckets {
		tier.Logger.Info(
			"Updating redis keys for aggregate",
			zap.String("aggregate", string(h.Name())),
			zap.Int("num_keys", logKeyCount[h]),
		)
	}
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
	period    uint64
	retention uint64
}

func NewTwoLevelStorage(period, retention uint64) BucketStore {
	return twoLevelRedisStore{
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
	aggname ftypes.AggName
	key     string
	id      uint64
}

func (t twoLevelRedisStore) GetBucketStore() BucketStore {
	return twoLevelRedisStore{period: t.period, retention: 0}
}

// unique takes a bucket and if the group this bucket belongs to, is absent in the map 'seen'
// it adds a key for that group and marks it seen
func (t twoLevelRedisStore) unique(
	name ftypes.AggName, b Bucket, seen map[group]int, rkeys []string,
) (map[group]int, []string, slot, error) {
	s, err := t.toSlot(name, &b)
	if err != nil {
		return nil, nil, slot{}, err
	}
	if _, ok := seen[s.g]; !ok {
		rkeys = append(rkeys, t.redisKey(name, s.g))
		seen[s.g] = len(rkeys) - 1
	}
	return seen, rkeys, s, err
}

func (t twoLevelRedisStore) getRaw(
	ctx context.Context, tier tier.Tier, names []ftypes.AggName, buckets []Bucket, defaults []value.Value,
) ([]value.Value, error) {
	// track seen groups, so we do not send duplicate groups to redis
	// 'seen' maps group to index in the array of groups sent to redis
	seen := make(map[group]int)
	var rkeys []string
	slots := make([]slot, len(buckets))
	var err error
	for i, b := range buckets {
		seen, rkeys, slots[i], err = t.unique(names[i], b, seen, rkeys)
		if err != nil {
			return nil, err
		}
	}
	// now load all groups from redis and get values from relevant slots
	groupVals, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	logVals := make([]value.Value, len(groupVals))
	ret := make([]value.Value, len(buckets))
	for i, s := range slots {
		ptr := seen[s.g]
		slotDict, err := t.getDict(groupVals[ptr])
		if err != nil {
			return nil, err
		}
		logVals[ptr] = slotDict
		ret[i] = t.getVal(s, slotDict, defaults[i])
	}
	return ret, nil
}

func (t twoLevelRedisStore) Get(
	ctx context.Context, tier tier.Tier, h Histogram, buckets []Bucket,
) ([]value.Value, error) {
	n := len(buckets)
	names := make([]ftypes.AggName, n)
	defaults := make([]value.Value, n)
	for i := range names {
		names[i] = h.Name()
		defaults[i] = h.Zero()
	}
	return t.getRaw(ctx, tier, names, buckets, defaults)
}

func (t twoLevelRedisStore) GetMulti(
	ctx context.Context, tier tier.Tier, buckets map[Histogram][]Bucket,
) (map[Histogram][]value.Value, error) {
	// track seen groups, so we do not send duplicate groups to redis
	// 'seen' maps group to index in the array of groups sent to redis
	seen := make(map[group]int)
	var rkeys []string
	slots := make(map[Histogram][]slot, len(buckets))
	var err error
	for h, buckets := range buckets {
		slots[h] = make([]slot, len(buckets))
		for i, b := range buckets {
			seen, rkeys, slots[h][i], err = t.unique(h.Name(), b, seen, rkeys)
			if err != nil {
				return nil, err
			}
		}
	}
	// now load all groups from redis and get values from relevant slots
	groupVals, err := tier.Redis.MGet(ctx, rkeys...)
	logVals := make([]value.Value, len(groupVals))
	ret := make(map[Histogram][]value.Value, len(buckets))
	if err != nil {
		return nil, err
	}
	for h, slots := range slots {
		for i, s := range slots {
			ptr := seen[s.g]
			slotDict, err := t.getDict(groupVals[ptr])
			if err != nil {
				return nil, err
			}
			logVals[ptr] = slotDict
			ret[h][i] = t.getVal(s, slotDict, h.Zero())
		}
	}
	t.logStats(logVals, "getBatched")
	return ret, nil
}

func (t twoLevelRedisStore) Set(ctx context.Context, tier tier.Tier, h Histogram, buckets []Bucket) error {
	// track seen groups, so we do not send duplicate groups to redis
	// 'seen' maps group to index in the array of groups sent to redis
	seen := make(map[group]int)
	var rkeys []string
	slots := make([]slot, len(buckets))
	var err error
	for i, b := range buckets {
		seen, rkeys, slots[i], err = t.unique(h.Name(), b, seen, rkeys)
		if err != nil {
			return err
		}
	}
	// now load all groups from redis first and update relevant slots
	groupVals, err := tier.Redis.MGet(ctx, rkeys...)
	newGroupVals := make([]value.Value, len(groupVals))
	if err != nil {
		return err
	}
	for _, s := range slots {
		ptr := seen[s.g]
		slotDict, err := t.getDict(groupVals[ptr])
		if err != nil {
			return err
		}
		newGroupVals[ptr] = t.setVal(s, slotDict)
	}
	// we set each key with a ttl of retention seconds
	ttls := make([]time.Duration, len(rkeys))
	for i := range ttls {
		ttls[i] = time.Second * time.Duration(t.retention)
	}
	t.logStats(newGroupVals, "set")
	tier.Logger.Info(
		"Updating redis keys for aggregate",
		zap.String("aggregate", string(h.Name())),
		zap.Int("num_keys", len(rkeys)),
	)
	return setInRedis(ctx, tier, rkeys, newGroupVals, ttls)
}

func (t twoLevelRedisStore) SetMulti(ctx context.Context, tier tier.Tier, buckets map[Histogram][]Bucket) error {
	// track seen groups, so we do not send duplicate groups to redis
	// 'seen' maps group to index in the array of groups sent to redis
	seen := make(map[group]int)
	var rkeys []string
	slots := make(map[Histogram][]slot, len(buckets))
	logKeyCount := make(map[Histogram]int, len(buckets))
	var err error
	for h, buckets := range buckets {
		slots[h] = make([]slot, len(buckets))
		for i, b := range buckets {
			c := len(rkeys)
			seen, rkeys, slots[h][i], err = t.unique(h.Name(), b, seen, rkeys)
			if err != nil {
				return err
			}
			if c < len(rkeys) {
				logKeyCount[h]++
			}
		}
	}
	// now load all groups from redis first and update relevant slots
	groupVals, err := tier.Redis.MGet(ctx, rkeys...)
	newGroupVals := make([]value.Value, len(groupVals))
	if err != nil {
		return err
	}
	for _, slots := range slots {
		for _, s := range slots {
			ptr := seen[s.g]
			slotDict, err := t.getDict(groupVals[ptr])
			if err != nil {
				return err
			}
			newGroupVals[ptr] = t.setVal(s, slotDict)
		}
	}
	// we set each key with a ttl of retention seconds
	ttls := make([]time.Duration, len(rkeys))
	for i := range ttls {
		ttls[i] = time.Second * time.Duration(t.retention)
	}
	t.logStats(newGroupVals, "setMulti")
	for h := range buckets {
		tier.Logger.Info(
			"Updating redis keys for aggregate",
			zap.String("aggregate", string(h.Name())),
			zap.Int("num_keys", logKeyCount[h]),
		)
	}
	return setInRedis(ctx, tier, rkeys, newGroupVals, ttls)
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

func (t twoLevelRedisStore) getDict(val interface{}) (d value.Dict, err error) {
	slotVal, err := interpretRedisResponse(val, value.NewDict(map[string]value.Value{}))
	if err != nil {
		return d, err
	}
	slotDict, ok := slotVal.(value.Dict)
	if !ok {
		return d, fmt.Errorf("could not read data: expected dict but found: %v\n", slotVal)
	}
	return slotDict, nil
}

func (t twoLevelRedisStore) getVal(s slot, d value.Dict, def value.Value) value.Value {
	idxStr := t.slotKey(s)
	v, ok := d.Get(idxStr)
	if !ok {
		v = def
	}
	return v
}

func (t twoLevelRedisStore) setVal(s slot, d value.Dict) value.Dict {
	idxStr := t.slotKey(s)
	d.Set(idxStr, s.val)
	return d
}

func toBuf(n uint64) []byte {
	buf := make([]byte, 8)
	k := binary.PutUvarint(buf, n)
	return buf[:k]
}

func (t twoLevelRedisStore) redisKey(name ftypes.AggName, g group) string {
	return fmt.Sprintf("agg_l2:%s:%s:%v%v", name, g.key, toBuf(t.period), toBuf(g.id))
}

func (t twoLevelRedisStore) toSlot(name ftypes.AggName, b *Bucket) (slot, error) {
	d := toDuration(b.Window) * b.Width
	if t.period%d != 0 {
		return slot{}, fmt.Errorf("can only store buckets with width that can fully fit in period of: '%d'sec", t.period)
	}
	startTs := d * b.Index
	gap := startTs % t.period
	return slot{
		g: group{
			aggname: name,
			key:     b.Key,
			id:      startTs / t.period,
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

func (t twoLevelRedisStore) logStats(groupVals []value.Value, mode string) {
	valsPerKey := 0
	count := 0
	for i := range groupVals {
		if asdict, ok := groupVals[i].(value.Dict); ok {
			valsPerKey += asdict.Len()
			count += 1
		}
	}
	metrics.WithLabelValues(
		fmt.Sprintf("l2_num_vals_per_key_in_%s", mode)).Observe(float64(valsPerKey) / float64(count))
}

var _ BucketStore = twoLevelRedisStore{}

func Update(ctx context.Context, tier tier.Tier, h Histogram, buckets []Bucket) error {
	cur, err := h.Get(ctx, tier, h, buckets)
	if err != nil {
		return err
	}
	for i := range cur {
		merged, err := h.Merge(cur[i], buckets[i].Value)
		if err != nil {
			return err
		}
		buckets[i].Value = merged
	}
	return h.Set(ctx, tier, h, buckets)
}

//==========================================================
// Private helpers for talking to redis
//==========================================================

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
	keySize, valSize := 0, 0
	vals := make([]interface{}, len(rkeys))
	for i := range rkeys {
		s := value.ToJSON(values[i])
		vals[i] = s
		keySize += len(rkeys[i])
		valSize += len(s)
	}
	metrics.WithLabelValues("redis_key_size_bytes").Observe(float64(keySize))
	metrics.WithLabelValues("redis_value_size_bytes").Observe(float64(valSize))
	return tier.Redis.MSet(ctx, rkeys, vals, ttls)
}
