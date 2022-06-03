package counter

import (
	"context"
	"fennel/lib/arena"
	"fmt"
	"strings"
	"sync"
	"time"

	// TODO: consider implementing own library in the future since the repository is old
	// and probably not maintained
	"github.com/mtraver/base91"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"fennel/lib/codex"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/redis"
	"fennel/tier"
)

const (
	// counterCodec is used to differentiate keys in potentially different schemas
	// with counterCodec = 1, the schema is: <agg_id>:<group_key>:<period>:<group_id>
	counterCodec codex.Codex = 1
	// with counterCodec = 2 the schema is : <agg_id>:<group_key>:<group_id>
	counterCodec2 codex.Codex = 2
	counterCodec3 codex.Codex = 3

	// slotCodec is used to differentiate keys in potentially different schemas
	// with slotCodec = 1, the schema is: <width>:<index> for `Window_MINUTE` and <window>:<width>:<index> for rest.
	slotCodec codex.Codex = 1

	// redisKey delimiter
	redisKeyDelimiter string = "-"
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

// slotArena is a pool of slices of type slot such that max cap of any slice is upto 1 << 12 (i.e. 4096)
// and total cap of all slices in pools is upto 1 << 24 i.e. ~4M. Since each slot is 64 bytes, this
// arena's total size is at most 4M * 64B = 256MB
var slotArena = arena.New[slot](1<<12, 1<<22)

// seenMapPool is a pool of maps from group -> int
var seenMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[group]int)
	},
}

func allocSeenMap() map[group]int {
	return seenMapPool.Get().(map[group]int)
}

func freeSeenMap(s map[group]int) {
	for k := range s {
		delete(s, k)
	}
	seenMapPool.Put(s)
}

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

// 68 bytes
type slot struct {
	window ftypes.Window // 4 bytes
	idx    int           // 4 bytes
	width  uint64        // 8 bytes
	val    value.Value   // 16 bytes
	g      group         // 36 bytes
}

// 36 bytes
type group struct {
	key   string       // 24 bytes
	id    uint64       // 8 bytes
	aggId ftypes.AggId // 4 bytes
}

func minSlotKey(width uint64, idx int) (string, error) {
	buf := arena.Bytes.Alloc(24, 24) // 8 + 8 + 8 for code, width, idx
	defer arena.Bytes.Free(buf)
	curr := 0
	if n, err := slotCodec.Write(buf[curr:]); err != nil {
		return "", err
	} else {
		curr += n
	}
	if n, err := binary.PutUvarint(buf[curr:], width); err != nil {
		return "", err
	} else {
		curr += n
	}
	if n, err := binary.PutVarint(buf[curr:], int64(idx)); err != nil {
		return "", err
	} else {
		curr += n
	}
	return base91.StdEncoding.EncodeToString(buf[:curr]), nil
}

func slotKey(window ftypes.Window, width uint64, idx int) (string, error) {
	buf := arena.Bytes.Alloc(32, 32) // 8+8+8+8 for codec, window, width, idx
	defer arena.Bytes.Free(buf)
	curr := 0
	if n, err := slotCodec.Write(buf[curr:]); err != nil {
		return "", err
	} else {
		curr += n
	}
	if n, err := binary.PutVarint(buf[curr:], int64(window)); err != nil {
		return "", err
	} else {
		curr += n
	}
	if n, err := binary.PutUvarint(buf[curr:], width); err != nil {
		return "", err
	} else {
		curr += n
	}
	if n, err := binary.PutVarint(buf[curr:], int64(idx)); err != nil {
		return "", err
	} else {
		curr += n
	}
	return base91.StdEncoding.EncodeToString(buf[:curr]), nil
}

func (t twoLevelRedisStore) GetBucketStore() BucketStore {
	return twoLevelRedisStore{period: t.period}
}

func (t twoLevelRedisStore) get(
	ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, buckets []counter.Bucket, defaults []value.Value,
) ([]value.Value, error) {
	// track seen groups, so we do not send duplicate groups to redis
	// 'seen' maps group to index in the array of groups sent to redis
	seen := allocSeenMap()
	defer freeSeenMap(seen)
	var rkeys []string
	var slots []slot
	slots = slotArena.Alloc(len(buckets), len(buckets)) // slots is a slice with length/cap of len(buckets)
	defer slotArena.Free(slots)

	// TODO: Consider creating a large enough buffer and construct slotKey and redisKey using partitions of the buffer
	// to save some CPU cycles
	for i, b := range buckets {
		s, err := t.toSlot(aggIds[i], &b)
		if err != nil {
			return nil, err
		}
		slots[i] = s
		if _, ok := seen[s.g]; !ok {
			rkey, err := t.redisKey(s.g)
			if err != nil {
				return nil, err
			}
			rkeys = append(rkeys, rkey)
			seen[s.g] = len(rkeys) - 1
		}
	}
	// now load all groups from redis and get values from relevant slots
	defaults_ := make([]value.Value, len(rkeys))
	for i := range defaults_ {
		defaults_[i] = value.NewDict(nil)
	}
	groupVals, err := readFromRedis(ctx, tier, rkeys, defaults_)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(buckets))
	for i, s := range slots {
		ptr := seen[s.g]
		slotDict, ok := groupVals[ptr].(value.Dict)
		if !ok {
			return nil, fmt.Errorf("could not read data: expected dict but found: %v", groupVals[ptr])
		}
		idxStr, err := t.slotKey(s)
		if err != nil {
			return nil, err
		}
		ret[i], ok = slotDict.Get(idxStr)
		if !ok {
			ret[i] = defaults[i]
		}
	}
	t.logStats(groupVals, "get")
	return ret, nil
}

func (t twoLevelRedisStore) Get(
	ctx context.Context, tier tier.Tier, aggId ftypes.AggId, buckets []counter.Bucket, default_ value.Value,
) ([]value.Value, error) {
	ctx, tmr := timer.Start(ctx, tier.ID, "twolevelredis.get")
	defer tmr.Stop()
	n := len(buckets)
	ids := make([]ftypes.AggId, n)
	defaults := make([]value.Value, n)
	for i := range ids {
		ids[i] = aggId
		defaults[i] = default_
	}
	return t.get(ctx, tier, ids, buckets, defaults)
}

func (t twoLevelRedisStore) GetMulti(
	ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, buckets [][]counter.Bucket, defaults []value.Value,
) ([][]value.Value, error) {
	ctx, tmr := timer.Start(ctx, tier.ID, "twolevelredis.get_multi")
	defer tmr.Stop()
	sz := 0
	for i := range buckets {
		sz += len(buckets[i])
	}
	ids_ := make([]ftypes.AggId, sz)
	buckets_ := make([]counter.Bucket, sz)
	defaults_ := make([]value.Value, sz)

	curr := 0
	for i := range buckets {
		for _, b := range buckets[i] {
			ids_[curr] = aggIds[i]
			buckets_[curr] = b
			defaults_[curr] = defaults[i]
			curr++
		}
	}
	vals, err := t.get(ctx, tier, ids_, buckets_, defaults_)
	if err != nil {
		return nil, err
	}
	ret := make([][]value.Value, len(aggIds))
	cur := 0
	for i := range buckets {
		ret[i] = make([]value.Value, len(buckets[i]))
		for j := range buckets[i] {
			ret[i][j] = vals[cur]
			cur++
		}
	}
	return ret, nil
}

func (t twoLevelRedisStore) set(ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, buckets []counter.Bucket) error {
	// track seen groups, so we do not send duplicate groups to redis
	// 'seen' maps group to index in the array of groups sent to redis
	seen := make(map[group]int, len(buckets))
	var rkeys []string
	slots := make([]slot, len(buckets))
	keyCount := make(map[ftypes.AggId]int, len(buckets))

	// TODO: Consider creating a large enough buffer and construct slotKey and redisKey using partitions of the buffer
	// to save some CPU cycles
	for i, b := range buckets {
		s, err := t.toSlot(aggIds[i], &b)
		if err != nil {
			return err
		}
		slots[i] = s
		if _, ok := seen[s.g]; !ok {
			rkey, err := t.redisKey(s.g)
			if err != nil {
				return err
			}
			rkeys = append(rkeys, rkey)
			seen[s.g] = len(rkeys) - 1
			keyCount[aggIds[i]]++
		}
	}
	// now load all groups from redis first and update relevant slots
	defaults := make([]value.Value, len(rkeys))
	for i := range defaults {
		defaults[i] = value.NewDict(nil)
	}
	groupVals, err := readFromRedis(ctx, tier, rkeys, defaults)
	if err != nil {
		return err
	}
	for _, s := range slots {
		ptr := seen[s.g]
		slotDict, ok := groupVals[ptr].(value.Dict)
		if !ok {
			return fmt.Errorf("could not read data: expected dict but found: %v", groupVals[ptr])
		}
		idxStr, err := t.slotKey(s)
		if err != nil {
			return err
		}
		slotDict.Set(idxStr, s.val)
		groupVals[ptr] = slotDict
	}
	// we set each key with a ttl of retention seconds
	ttls := make([]time.Duration, len(rkeys))
	for i := range ttls {
		ttls[i] = time.Second * time.Duration(t.retention)
	}
	t.logStats(groupVals, "set")
	for name, numKeys := range keyCount {
		tier.Logger.Info(
			"Updating redis keys for aggregate", zap.Int("aggregate", int(name)), zap.Int("num_keys", numKeys),
		)
	}
	return setInRedis(ctx, tier, rkeys, groupVals, ttls)
}

func (t twoLevelRedisStore) Set(ctx context.Context, tier tier.Tier, aggId ftypes.AggId, buckets []counter.Bucket) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "twolevelredis.set")
	defer tmr.Stop()
	ids := make([]ftypes.AggId, len(buckets))
	for i := range ids {
		ids[i] = aggId
	}
	return t.set(ctx, tier, ids, buckets)
}

func (t twoLevelRedisStore) SetMulti(
	ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, buckets [][]counter.Bucket) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "twolevelredis.set_multi")
	defer tmr.Stop()
	var ids_ []ftypes.AggId
	var buckets_ []counter.Bucket
	for i := range buckets {
		for _, b := range buckets[i] {
			ids_ = append(ids_, aggIds[i])
			buckets_ = append(buckets_, b)
		}
	}
	return t.set(ctx, tier, ids_, buckets_)
}

func (t twoLevelRedisStore) slotKey(s slot) (string, error) {
	// since minute is the more common type which also produces a lot of keys,
	// we use this window by default to save couple more bytes per slot key
	if s.window == ftypes.Window_MINUTE {
		return minSlotKey(s.width, s.idx)
	} else {
		return slotKey(s.window, s.width, s.idx)
	}
}

// redisKey returns key for an redis entry corresponding to the given aggregate id and group
//
// encoding is as follows:
// 	{AggrId}-{Codec}-{GroupIdentifier}
//
// where:
// 	AggrId -> base91 encoded, UvarInt serialized AggId
//	Codec  -> base91 encoded, UvarInt serialized codec; codec refers to the encoding mechanism of the group identifier
//  GroupIdentifier -> base91 encoded (NOTE: this is determined with Codec), serialization of group key + storage period + group id
//
// we use `-` (omitted from base91 character set) as the delimiter b/w different parts of the key to prefix search for a particular
// AggId or (AggId, Codec) pair on redis
func (t twoLevelRedisStore) redisKey(g group) (string, error) {
	var aggStr, codecStr, groupIdStr string
	// aggId
	{
		aggBuf := make([]byte, 8) // aggId
		curr, err := binary.PutUvarint(aggBuf, uint64(g.aggId))
		if err != nil {
			return "", err
		}
		aggStr = base91.StdEncoding.EncodeToString(aggBuf[:curr])
	}
	// codec
	{
		codecBuf := make([]byte, 8) // codec
		curr, err := counterCodec.Write(codecBuf)
		if err != nil {
			return "", err
		}
		codecStr = base91.StdEncoding.EncodeToString(codecBuf[:curr])
	}
	// groupid
	{
		groupIdBuf := make([]byte, 8+len(g.key)+8+8) // (length of groupkey) + groupkey + period + groupid
		curr := 0
		if n, err := binary.PutString(groupIdBuf[curr:], g.key); err != nil {
			return "", err
		} else {
			curr += n
		}
		if n, err := binary.PutUvarint(groupIdBuf[curr:], t.period); err != nil {
			return "", err
		} else {
			curr += n
		}
		if n, err := binary.PutUvarint(groupIdBuf[curr:], g.id); err != nil {
			return "", err
		} else {
			curr += n
		}
		groupIdStr = base91.StdEncoding.EncodeToString(groupIdBuf[:curr])
	}

	// concatenate the base91 encoded strings with `-` as the delimiter
	sb := strings.Builder{}
	sb.Grow(len(aggStr) + len(codecStr) + len(groupIdStr) + 2) // allocate 2 bytes for delimiter
	sb.WriteString(aggStr)
	sb.WriteString(redisKeyDelimiter)
	sb.WriteString(codecStr)
	sb.WriteString(redisKeyDelimiter)
	sb.WriteString(groupIdStr)
	return sb.String(), nil
}

func (t twoLevelRedisStore) toSlot(id ftypes.AggId, b *counter.Bucket) (slot, error) {
	d := toDuration(b.Window) * b.Width
	if t.period%d != 0 {
		return slot{}, fmt.Errorf("can only store buckets with width that can fully fit in period of: '%d'sec", t.period)
	}
	startTs := d * b.Index
	gap := startTs % t.period
	return slot{
		g: group{
			aggId: id,
			key:   b.Key,
			id:    startTs / t.period,
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

func Update(ctx context.Context, tier tier.Tier, aggId ftypes.AggId, buckets []counter.Bucket, h Histogram) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "counter.update")
	defer tmr.Stop()
	cur, err := h.Get(ctx, tier, aggId, buckets, h.Zero())
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
	return h.Set(ctx, tier, aggId, buckets)
}

// ==========================================================
// Private helpers for talking to redis
// ==========================================================

func readFromRedis(ctx context.Context, tier tier.Tier, rkeys []string, defaults []value.Value) ([]value.Value, error) {
	res, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(rkeys))
	for i, v := range res {
		if ret[i], err = interpretRedisResponse(v, defaults[i].Clone()); err != nil {
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
		var val value.Value
		err := value.Unmarshal([]byte(t), &val)
		return val, err
	default:
		return nil, fmt.Errorf("unexpected type from redis")
	}
}

func setInRedis(ctx context.Context, tier tier.Tier, rkeys []string, values []value.Value, ttls []time.Duration) error {
	if len(rkeys) != len(values) || len(rkeys) != len(ttls) {
		return fmt.Errorf("can not set in redis: keys, values, ttls should be of equal length")
	}
	keySize, valSize := 0, 0
	vals := make([]interface{}, len(rkeys))
	for i := range rkeys {
		s, err := value.Marshal(values[i])
		if err != nil {
			return err
		}
		vals[i] = s
		keySize += len(rkeys[i])
		valSize += len(s)
	}
	metrics.WithLabelValues("redis_key_size_bytes").Observe(float64(keySize))
	metrics.WithLabelValues("redis_value_size_bytes").Observe(float64(valSize))
	return tier.Redis.MSet(ctx, rkeys, vals, ttls)
}
