package counter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fennel/lib/arena"
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

	// Batch size (number of keys to call from redis) for GetMulti
	batchSize = 10000
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

var bucket_stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bucket_stats",
	Help: "Stats number of buckets being computed for every aggregate",
}, []string{"aggregate_id"})

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
	period    uint32
	retention uint64
}

func NewTwoLevelStorage(period uint32, retention uint64) BucketStore {
	return twoLevelRedisStore{
		period:    period,
		retention: retention,
	}
}

// 60 bytes
type slot struct {
	window ftypes.Window // 4 bytes
	idx    int           // 4 bytes
	width  uint32        // 4 bytes
	val    value.Value   // 16 bytes
	g      group         // 32 bytes
}

// 32 bytes
type group struct {
	key   string       // 24 bytes
	id    uint32       // 4 bytes
	aggId ftypes.AggId // 4 bytes
}

func minSlotKey(width uint32, idx int) (string, error) {
	arr := [24]byte{}
	buf := arr[:] // 8 + 8 + 8 for code, width, idx
	curr := 0
	if n, err := slotCodec.Write(buf[curr:]); err != nil {
		return "", err
	} else {
		curr += n
	}
	if n, err := binary.PutUvarint(buf[curr:], uint64(width)); err != nil {
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

func slotKey(window ftypes.Window, width uint32, idx int) (string, error) {
	arr := [32]byte{}
	buf := arr[:] // 8+8+8+8 for codec, window, width, idx
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
	if n, err := binary.PutUvarint(buf[curr:], uint64(width)); err != nil {
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

type ptr struct {
	slotId []int
	window []ftypes.Window
	width  []uint32
	aggPtr []int
	posPtr []int
}

func (p *ptr) addToPtr(slotId int, window ftypes.Window, width uint32, aggPtr int, posPtr int) {
	p.slotId = append(p.slotId, slotId)
	p.window = append(p.window, window)
	p.width = append(p.width, width)
	p.aggPtr = append(p.aggPtr, aggPtr)
	p.posPtr = append(p.posPtr, posPtr)
}

func (t twoLevelRedisStore) GetMulti(ctx context.Context, tier tier.Tier,
	aggIds []ftypes.AggId, bucketLists [][]counter.BucketList, defaults []value.Value,
) (vals [][]value.Value, err error) {
	ctx, tmr := timer.Start(ctx, tier.ID, "twolevelredis.get_multi")
	defer tmr.Stop()
	if len(aggIds) == 0 {
		return vals, err
	}
	vals = make([][]value.Value, len(aggIds))
	for i := range bucketLists {
		totalSize := 0
		for _, bl := range bucketLists[i] {
			totalSize += int(bl.Count())
		}
		vals[i] = make([]value.Value, totalSize)
		bucket_stats.WithLabelValues(fmt.Sprint(aggIds[i])).Set(float64(totalSize))
	}
	curs := make([]int, len(aggIds))

	// We want to process same aggIds together.
	// aggMap[aggId] maps to a slice that contains all the indices of aggId.
	aggReps := make(map[ftypes.AggId]int, len(aggIds))
	for _, aggId := range aggIds {
		aggReps[aggId]++
	}
	aggMap := make(map[ftypes.AggId][]int, len(aggReps))
	for aggId, n := range aggReps {
		aggMap[aggId] = make([]int, 0, n)
	}
	for i, aggId := range aggIds {
		aggMap[aggId] = append(aggMap[aggId], i)
	}

	// We request redis for at most batchSize keys at a time. Aggregates are processed in order, groups already
	// seen are not added to the buffer, and when the number of groups to query from redis reaches bufsize,
	// we query redis and fill vals with those values.
	// TODO: Consider creating a large enough buffer and construct keys using partitions of the buffer
	// to save some CPU cycles
	numBatches := 0
	seen := make(map[group]int, batchSize)
	keyCtr := 0
	rkeys := make([]string, batchSize)
	// keep pointers to note which slot's value to return to which get request of the batch
	ptrs := make([]ptr, batchSize)
	for aggId, indices := range aggMap {
		for _, i := range indices {
			for _, bl := range bucketLists[i] {
				// convert bucket lists to groups and also get the indices of the groups requested by this bucket list
				groups, locs, err := t.toGroups(aggId, bl)
				if err != nil {
					return nil, err
				}
				for j, g := range groups {
					if p, ok := seen[g]; ok {
						// if we have already seen this group, then we just need to update the ptrs
						for _, loc := range locs[j] {
							ptrs[p].addToPtr(loc, bl.Window, bl.Width, i, curs[i])
							curs[i]++
						}
						continue
					}
					seen[g] = keyCtr // mark group as seen
					rkeys[keyCtr], err = t.redisKey(g)
					for _, loc := range locs[j] {
						ptrs[keyCtr].addToPtr(loc, bl.Window, bl.Width, i, curs[i]) // update ptrs
						curs[i]++
					}
					keyCtr++ // increment batch counter

					// batch is full, so we get values now
					if keyCtr == batchSize {
						err := t.get(ctx, tier, rkeys, ptrs, defaults, vals)
						if err != nil {
							return nil, err
						}
						numBatches++
						keyCtr = 0
						seen = make(map[group]int, batchSize)
					}
				}
			}
		}
	}
	// if batch is not empty at the end, get the remaining values
	if keyCtr != 0 {
		err := t.get(ctx, tier, rkeys[:keyCtr], ptrs[:keyCtr], defaults, vals)
		if err != nil {
			return nil, err
		}
		numBatches++
	}
	metrics.WithLabelValues("l2_num_batches_per_get_multi").Observe(float64(numBatches))

	return vals, err
}

// get performs one batch call for GetMulti and fills values into the 2D slice provided by it
func (t twoLevelRedisStore) get(
	ctx context.Context, tier tier.Tier, rkeys []string, ptrs []ptr, defaults []value.Value, vals [][]value.Value,
) error {
	rvals, err := readFromRedis(ctx, tier, rkeys)
	if err != nil {
		return err
	}
	for p, v := range rvals {
		vDict, ok := v.(value.Dict)
		if !ok {
			return fmt.Errorf("could not read data: expected dict but found: %v", v)
		}
		for q, sId := range ptrs[p].slotId {
			aPtr, pPtr := ptrs[p].aggPtr[q], ptrs[p].posPtr[q]
			idxStr, err := t.slotKey2(ptrs[p].window[q], ptrs[p].width[q], sId)
			if err != nil {
				return err
			}
			val, ok := vDict.Get(idxStr)
			if !ok {
				val = defaults[aPtr]
			}
			vals[aPtr][pPtr] = val
		}
	}
	t.logStats(rvals, "get")
	return nil
}

func (t twoLevelRedisStore) toGroups(aggId ftypes.AggId, bList counter.BucketList) ([]group, [][]int, error) {
	d := bList.Width * toDuration(bList.Window)
	if t.period%d != 0 {
		return nil, nil, fmt.Errorf(
			"can only store buckets with width that can fully fit in period of: '%d'sec", t.period,
		)
	}
	n := t.period / (bList.Width * toDuration(bList.Window)) // number of buckets in a period
	start := bList.StartIndex / n
	end := bList.EndIndex / n
	groups := make([]group, end-start+1)
	locs := make([][]int, end-start+1)
	for i := start; i <= end; i++ {
		groups[i-start] = group{
			key:   bList.Key,
			id:    i,
			aggId: aggId,
		}
		s, e := i*n, (i+1)*n-1
		if s < bList.StartIndex {
			s = bList.StartIndex
		}
		if e > bList.EndIndex {
			e = bList.EndIndex
		}
		locs[i-start] = make([]int, e-s+1)
		for j := s; j <= e; j++ {
			locs[i-start][j-s] = int(j % n)
		}
	}
	return groups, locs, nil
}

func (t twoLevelRedisStore) set(ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, buckets []counter.Bucket, values []value.Value) error {
	// track seen groups, so we do not send duplicate groups to redis
	// 'seen' maps group to index in the array of groups sent to redis
	seen := make(map[group]int, len(buckets))
	var rkeys []string
	slots := make([]slot, len(buckets))
	keyCount := make(map[ftypes.AggId]int, len(buckets))

	for i, b := range buckets {
		s := &slots[i]
		s.val = values[i]
		err := t.toSlot(aggIds[i], &b, s)
		if err != nil {
			return err
		}
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
	groupVals, err := readFromRedis(ctx, tier, rkeys)
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

func (t twoLevelRedisStore) SetMulti(
	ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, buckets [][]counter.Bucket, values [][]value.Value) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "twolevelredis.set_multi")
	defer tmr.Stop()
	var ids_ []ftypes.AggId
	var buckets_ []counter.Bucket
	var values_ []value.Value
	for i := range buckets {
		for j, b := range buckets[i] {
			ids_ = append(ids_, aggIds[i])
			buckets_ = append(buckets_, b)
			values_ = append(values_, values[i][j])
		}
	}
	return t.set(ctx, tier, ids_, buckets_, values_)
}

func (t twoLevelRedisStore) slotKey2(window ftypes.Window, width uint32, idx int) (string, error) {
	if window == ftypes.Window_MINUTE {
		return minSlotKey(width, idx)
	} else {
		return slotKey(window, width, idx)
	}
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
		arr := [8]byte{}
		aggBuf := arr[:] // aggId
		curr, err := binary.PutUvarint(aggBuf, uint64(g.aggId))
		if err != nil {
			return "", err
		}
		aggStr = base91.StdEncoding.EncodeToString(aggBuf[:curr])
	}
	// codec
	{
		arr := [8]byte{}
		codecBuf := arr[:] // codec
		curr, err := counterCodec.Write(codecBuf)
		if err != nil {
			return "", err
		}
		codecStr = base91.StdEncoding.EncodeToString(codecBuf[:curr])
	}
	// groupid
	{
		sz := 24 + len(g.key)
		groupIdBuf := arena.Bytes.Alloc(sz, sz) // (length of groupkey) + groupkey + period + groupid
		defer arena.Bytes.Free(groupIdBuf)
		curr := 0
		if n, err := binary.PutString(groupIdBuf[curr:], g.key); err != nil {
			return "", err
		} else {
			curr += n
		}
		if n, err := binary.PutUvarint(groupIdBuf[curr:], uint64(t.period)); err != nil {
			return "", err
		} else {
			curr += n
		}
		if n, err := binary.PutUvarint(groupIdBuf[curr:], uint64(g.id)); err != nil {
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

func (t twoLevelRedisStore) toSlot(id ftypes.AggId, b *counter.Bucket, s *slot) error {
	d := toDuration(b.Window) * b.Width
	if t.period%d != 0 {
		return fmt.Errorf("can only store buckets with width that can fully fit in period of: '%d'sec", t.period)
	}
	startTs := d * b.Index
	gap := startTs % t.period
	s.g = group{
		aggId: id,
		key:   b.Key,
		id:    startTs / t.period,
	}
	s.window = b.Window
	s.width = b.Width
	s.idx = int(gap / d)
	return nil
}

func toDuration(w ftypes.Window) uint32 {
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
	metrics.WithLabelValues(fmt.Sprintf("l2_num_vals_per_key_in_%s", mode)).Observe(
		float64(valsPerKey) / float64(count),
	)
	metrics.WithLabelValues(fmt.Sprintf("l2_num_vals_per_batch_in_%s", mode)).Observe(
		float64(valsPerKey),
	)
	metrics.WithLabelValues(fmt.Sprintf("l2_num_key_per_batch_in_%s", mode)).Observe(
		float64(count),
	)
}

var _ BucketStore = twoLevelRedisStore{}

func Update(ctx context.Context, tier tier.Tier, aggId ftypes.AggId, buckets []counter.Bucket, values []value.Value, h Histogram) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "counter.update")
	defer tmr.Stop()
	bucketLists := make([]counter.BucketList, len(buckets))
	for i, b := range buckets {
		bucketLists[i] = counter.BucketList{
			Key:        b.Key,
			Window:     b.Window,
			Width:      b.Width,
			StartIndex: b.Index,
			EndIndex:   b.Index,
		}
	}
	cur, err := h.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.BucketList{bucketLists}, []value.Value{h.Zero()})
	if err != nil {
		return err
	}

	// We fetch for only 1 aggregate, hence its a 2d array of 1 element
	defer arena.Values.Free(cur[0])
	for i := range cur[0] {
		values[i], err = h.Merge(cur[0][i], values[i])
		if err != nil {
			return err
		}
	}
	return h.SetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.Bucket{buckets}, [][]value.Value{values})
}

// ==========================================================
// Private helpers for talking to redis
// ==========================================================

func readFromRedis(ctx context.Context, tier tier.Tier, rkeys []string) ([]value.Value, error) {
	res, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(rkeys))
	for i, v := range res {
		if ret[i], err = interpretRedisResponse(v); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func interpretRedisResponse(v interface{}) (value.Value, error) {
	switch t := v.(type) {
	case nil:
		return value.NewDict(nil), nil
	case error:
		if t != redis.Nil {
			return nil, t
		} else {
			return value.NewDict(nil), nil
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
	metrics.WithLabelValues("l2_redis_key_size_bytes").Observe(float64(keySize))
	metrics.WithLabelValues("l2_redis_value_size_bytes").Observe(float64(valSize))
	return tier.Redis.MSet(ctx, rkeys, vals, ttls)
}
