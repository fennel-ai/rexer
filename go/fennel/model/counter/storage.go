package counter

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"fennel/lib/arena"
	"fennel/lib/codex"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/binary"
	"fennel/lib/utils/encoding/base91"
	"fennel/lib/value"
	"fennel/redis"
	"fennel/tier"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

const (
	// counterCodec is used to differentiate keys in potentially different schemas
	// with counterCodec = 1, the schema is: <agg_id>:<group_key>:<period>:<group_id>
	counterCodec codex.Codex = 1
	// with counterCodec = 2 the schema is : <agg_id>:<group_key>:<group_id>
	//counterCodec2 codex.Codex = 2
	//counterCodec3 codex.Codex = 3

	// slotCodec is used to differentiate keys in potentially different schemas
	// with slotCodec = 1, the schema is: <width>:<index> for `Window_MINUTE` and <window>:<width>:<index> for rest.
	slotCodec codex.Codex = 1

	// redisKey delimiter
	redisKeyDelimiter string = "-"

	// Max batch size (number of keys to call from redis) for GetMulti
	maxBatchSize = 5000
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

// groupArena is a pool of slices such that max cap of any slice is upto 1<<15 (i.e. 32K)
// and total cap of all slices in pools is upto 1 << 22 i.e, ~4M. Since each group is 32 bytes,
// this arena's total size is at most 4M * 32B = 128MB
var groupArena = arena.New[Group](1<<15, 1<<22)

// aggIdArena is a pool of slices such that max cap of any slice is upto 1<<14 (i.e. 16K)
// and total cap of all slices in pools is upto 1 << 20 i.e, ~1M. Since each aggId is 4 bytes,
// this arena's total size is at most 1M * 4B = 4MB
var aggIdArena = arena.New[ftypes.AggId](1<<14, 1<<20)

// seenMapPool is a pool of maps from group -> int
var seenMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[Group]int)
	},
}

func allocSeenMap() map[Group]int {
	return seenMapPool.Get().(map[Group]int)
}

func freeSeenMap(s map[Group]int) {
	for k := range s {
		delete(s, k)
	}
	seenMapPool.Put(s)
}

/*
	twoLevelRedisStore groups all keys that fall within a period and stores them as value.Dict in a single redis key
	within that dictionary, the key is derived from the index of the key within that Group
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
	retention uint32
}

func NewTwoLevelStorage(period uint32, retention uint32) BucketStore {
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
	g      Group         // 32 bytes
}

// 32 bytes
type Group struct {
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
	a := base91.StdEncoding.Encode(buf[:curr])
	return a, nil
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
	a := base91.StdEncoding.Encode(buf[:curr])
	return a, nil
}

func (t twoLevelRedisStore) GetBucketStore() BucketStore {
	return twoLevelRedisStore{period: t.period}
}

func (t twoLevelRedisStore) getAggIdToIndexListMap(aggIds []ftypes.AggId) map[ftypes.AggId][]int {
	// We want to process same aggIds together.
	// aggMap[aggId] maps to a slice that contains all the indices of aggIds whose value is aggId.
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
	return aggMap
}

// getBatchSize guesses the number of groups in the given bucketLists and returns
// that as batch size (or maxBatchSize whichever is smaller). This is so that we
// do not allocate a lot of memory for the batch if the number of elements is small.
func (t twoLevelRedisStore) getBatchSize(bucketLists [][]counter.BucketList) int {
	batchSize := 0
	for i := range bucketLists {
		for _, bl := range bucketLists[i] {
			n := t.period / (bl.Width * toDuration(bl.Window)) // number of buckets in a period
			startGroup := bl.StartIndex / n
			endGroup := bl.EndIndex / n
			batchSize += int(endGroup - startGroup + 1)
		}
	}
	if batchSize > maxBatchSize {
		return maxBatchSize
	}
	return batchSize
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
		vals[i] = arena.Values.Alloc(totalSize, totalSize)
		bucket_stats.WithLabelValues(fmt.Sprint(aggIds[i])).Set(float64(totalSize))
	}
	next := make([]int, len(aggIds)) // next[i] is the next unfilled index of the vals[i] slice
	aggMap := t.getAggIdToIndexListMap(aggIds)

	// We request redis for at most batchSize keys at a time. Aggregates are processed in order, groups already
	// seen are not added to the buffer, and when the number of groups to query from redis reaches batchSize,
	// we query redis and fill vals with those values.
	// TODO: Consider creating a large enough buffer and construct keys using partitions of the buffer
	// to save some CPU cycles
	numBatches := 0
	batchSize := t.getBatchSize(bucketLists)

	rkeys := arena.Strings.Alloc(batchSize, batchSize)
	defer arena.Strings.Free(rkeys)
	// fix order of iteration over aggregates
	aggOrder := aggIdArena.Alloc(len(aggMap), len(aggMap))
	i := 0
	for aggId := range aggMap {
		aggOrder[i] = aggId
		i++
	}

	/*
		t.get() starts processing groups from a specified starting point and stops when there are no
		more groups left to process, or when the max batch size is reached. It then gets the corresponding
		redis values and returns them along with the starting point for the next iteration.

		t.fill() starts processing groups from the same starting point and fills the values to be returned
		using the redis values obtained from the t.get() call.

		sx -> start at aggId = aggOrder[sx]
		sy -> start at i = aggMap[aggId][sy]
		sz -> start at bl = bucketLists[i][sz]
		sw -> start at g = groups[sw]
	*/
	sx, sy, sz, sw := 0, 0, 0, 0
	for sx != -1 {
		seen := allocSeenMap()
		rvals, ex, ey, ez, ew, err := t.get(ctx, tier, aggOrder, aggMap, bucketLists, sx, sy, sz, sw, batchSize, seen)
		if err != nil {
			return nil, err
		}
		err = t.fill(vals, rvals, aggOrder, aggMap, defaults, bucketLists, sx, sy, sz, sw, batchSize, next, seen)
		if err != nil {
			return nil, err
		}
		sx, sy, sz, sw = ex, ey, ez, ew
		numBatches++
		freeSeenMap(seen)
	}
	metrics.WithLabelValues("l2_num_batches_per_get_multi").Observe(float64(numBatches))
	aggIdArena.Free(aggOrder)

	return vals, err
}

func (t twoLevelRedisStore) get(
	ctx context.Context, tier tier.Tier, aggOrder []ftypes.AggId, aggMap map[ftypes.AggId][]int,
	bucketLists [][]counter.BucketList, sx, sy, sz, sw, batchSize int, seen map[Group]int,
) ([]value.Value, int, int, int, int, error) {
	rkeys := arena.Strings.Alloc(batchSize, batchSize)
	defer arena.Strings.Free(rkeys)

	keyCtr := 0
	ex, ey, ez, ew := -1, -1, -1, -1

Loop:
	for x := sx; x < len(aggOrder); x++ {
		aggId := aggOrder[x]
		indices := aggMap[aggId]
		for y := sy; y < len(indices); y++ {
			i := indices[y]
			for z := sz; z < len(bucketLists[i]); z++ {
				bl := bucketLists[i][z]
				groups, _, err := t.toGroups(aggId, bl, false)
				if err != nil {
					return nil, 0, 0, 0, 0, err
				}
				defer groupArena.Free(groups)
				for w := sw; w < len(groups); w++ {
					g := groups[w]
					if _, ok := seen[g]; ok {
						continue
					}
					if keyCtr == batchSize {
						ex, ey, ez, ew = x, y, z, w
						break Loop
					}
					seen[g] = keyCtr
					rkeys[keyCtr], err = t.redisKey(g)
					if err != nil {
						return nil, 0, 0, 0, 0, err
					}
					keyCtr++
				}
			}
		}
	}

	rvals, err := readFromRedis(ctx, tier, rkeys)
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	t.logStats(rvals, "get")
	return rvals, ex, ey, ez, ew, nil
}

func (t twoLevelRedisStore) fill(
	vals [][]value.Value, rvals []value.Value, aggOrder []ftypes.AggId, aggMap map[ftypes.AggId][]int,
	defaults []value.Value, bucketLists [][]counter.BucketList, sx, sy, sz, sw, batchSize int, next []int, seen map[Group]int,
) error {
Loop:
	for x := sx; x < len(aggOrder); x++ {
		aggId := aggOrder[x]
		indices := aggMap[aggId]
		for y := sy; y < len(indices); y++ {
			i := indices[y]
			for z := sz; z < len(bucketLists[i]); z++ {
				bl := bucketLists[i][z]
				groups, slotIds, err := t.toGroups(aggId, bl, true)
				if err != nil {
					return err
				}
				defer groupArena.Free(groups)
				for i := range slotIds {
					defer arena.Ints.Free(slotIds[i])
				}
				for w := sw; w < len(groups); w++ {
					g := groups[w]
					if _, ok := seen[g]; !ok {
						break Loop
					}
					for j := range slotIds[w] {
						idxStr, err := t.slotKey2(bl.Window, bl.Width, slotIds[w][j])
						if err != nil {
							return err
						}
						vd, ok := rvals[seen[g]].(value.Dict)
						if !ok {
							return fmt.Errorf("count not read data: expected dict but found: %v", rvals[seen[g]])
						}
						v, ok := vd.Get(idxStr)
						if !ok {
							v = defaults[i]
						}
						vals[i][next[i]] = v
						next[i]++
					}
				}
			}
		}
	}
	return nil
}

// toGroups takes a BucketList and returns all the groups of the twoLevelRedisStore whose range overlaps
// with this BucketList's range. It also returns all the relevant slotIds for each group.
func (t twoLevelRedisStore) toGroups(aggId ftypes.AggId, bList counter.BucketList, withSlots bool) ([]Group, [][]int, error) {
	d := bList.Width * toDuration(bList.Window)
	if t.period%d != 0 {
		return nil, nil, fmt.Errorf(
			"can only store buckets with width that can fully fit in period of: '%d'sec", t.period,
		)
	}
	n := t.period / (bList.Width * toDuration(bList.Window)) // number of buckets in a period
	start := bList.StartIndex / n
	end := bList.EndIndex / n
	c := int(end - start + 1)
	groups := groupArena.Alloc(c, c)
	var slotIds [][]int
	if withSlots {
		slotIds = make([][]int, c)
	}
	for i := start; i <= end; i++ {
		groups[i-start] = Group{
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
		if withSlots {
			slotIds[i-start] = arena.Ints.Alloc(int(e-s+1), int(e-s+1))
			for j := s; j <= e; j++ {
				// slotID = (Bucket Index) modulo (Number of Buckets per Group)
				slotIds[i-start][j-s] = int(j % n)
			}
		}
	}
	return groups, slotIds, nil
}

func (t twoLevelRedisStore) set(ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, buckets []counter.Bucket, values []value.Value) error {
	// track seen groups, so we do not send duplicate groups to redis
	// 'seen' maps Group to index in the array of groups sent to redis
	seen := make(map[Group]int, len(buckets))
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

// redisKey returns key for an redis entry corresponding to the given aggregate id and Group
//
// encoding is as follows:
// 	{AggrId}-{Codec}-{GroupIdentifier}
//
// where:
// 	AggrId -> base91 encoded, UvarInt serialized AggId
//	Codec  -> base91 encoded, UvarInt serialized codec; codec refers to the encoding mechanism of the Group identifier
//  GroupIdentifier -> base91 encoded (NOTE: this is determined with Codec), serialization of Group key + storage period + Group id
//
// we use `-` (omitted from base91 character set) as the delimiter b/w different parts of the key to prefix search for a particular
// AggId or (AggId, Codec) pair on redis
func (t twoLevelRedisStore) redisKey(g Group) (string, error) {
	var aggStr, codecStr, groupIdStr string
	// aggId
	{
		arr := [8]byte{}
		aggBuf := arr[:] // aggId
		curr, err := binary.PutUvarint(aggBuf, uint64(g.aggId))
		if err != nil {
			return "", err
		}
		aggStr = base91.StdEncoding.Encode(aggBuf[:curr])
	}
	// codec
	{
		arr := [8]byte{}
		codecBuf := arr[:] // codec
		curr, err := counterCodec.Write(codecBuf)
		if err != nil {
			return "", err
		}
		codecStr = base91.StdEncoding.Encode(codecBuf[:curr])
	}
	// groupid
	{
		sz := 24 + len(g.key)
		groupIdBuf := make([]byte, sz)
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
		groupIdStr = base91.StdEncoding.Encode(groupIdBuf[:curr])
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
	s.g = Group{
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

// ==========================================================
// Private helpers for talking to redis
// ==========================================================
var redisKeyStats = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "num_keys_queried",
	Help: "Number of keys queried in redis",
	Objectives: map[float64]float64{
		0.25: 0.05,
		0.50: 0.05,
		0.75: 0.05,
		0.90: 0.05,
		0.95: 0.02,
		0.99: 0.01,
	},
}, []string{"type"})

func readFromRedis(ctx context.Context, tier tier.Tier, rkeys []string) ([]value.Value, error) {
	_, tmrEntire := timer.Start(ctx, tier.ID, "redis.readFromRedis")
	defer tmrEntire.Stop()
	res, err := tier.Redis.MGet(ctx, rkeys...)
	if err != nil {
		return nil, err
	}
	redisKeyStats.WithLabelValues("redis_keys_interpreted").Observe(float64(len(res)))
	_, tmr := timer.Start(ctx, tier.ID, "redis.interpret_response")
	defer tmr.Stop()
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
		var err error
		tBytes := []byte(t)
		if len(tBytes) > 0 && tBytes[0] == value.REXER_CODEC_V1 {
			val, err = value.Unmarshal(tBytes)
			if err == nil {
				return val, nil
			}
		}
		err = value.ProtoUnmarshal(tBytes, &val)
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
