package counter

import (
	"context"
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"fennel/lib/arena"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/binary"
	"fennel/lib/utils/encoding/base91"
	"fennel/lib/value"
	"fennel/tier"

	"go.uber.org/zap"
)

/*
	fixedSplitBucketizer creates buckets by splitting the duration equally among a fixed number of buckets.
	It first computes width = duration / numBuckets. Then it divides the entire time range (not just the duration)
	with buckets that cover a time range equal to width. Each bucket has an index which covers the time from
	(index * width) to ((index+1)*width). Every point in time is covered by some bucket whose index is given by
	(curTimeInSeconds / width).

	splitStore divides the entire time range with groups that cover a time range of width bucketsPerGroup * bucketWidth.
	The first group contains buckets with indices in range [0, bucketsPerGroup) and so on. A group is stored as a
	redis hashmap. When the number of keys in a redis hashmap is small, it is stored very efficiently. So,
	bucketsPerGroup should not be too large. Redis configuration should be set accordingly.

	Ref: https://redis.io/docs/reference/optimization/memory-optimization/
*/
type splitStore struct {
	bucketsPerGroup uint32
	retention       uint64
}

func (s splitStore) GetBucketStore() BucketStore {
	return splitStore{bucketsPerGroup: s.bucketsPerGroup}
}

var _ BucketStore = splitStore{}

type splitGroup struct {
	aggID ftypes.AggId
	key   string
	pos   uint32
	width uint32
}

func (s splitStore) logStats(groups map[splitGroup][]string, mode string) {
	valsPerKey := 0
	count := 0
	for _, l := range groups {
		for i := range l {
			if len(l[i]) != 0 {
				valsPerKey++
			}
		}
		count++
	}
	metrics.WithLabelValues(fmt.Sprintf("split_store_num_vals_per_key_in%s", mode)).Observe(float64(valsPerKey) / float64(count))
}

func (s splitStore) GetMulti(
	ctx context.Context, tier tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, defaults []value.Value,
) ([][]value.Value, error) {
	ctx, t := timer.Start(ctx, tier.ID, "splitstore.get_multi")
	defer t.Stop()
	vals, err := s.getFromRedis(ctx, &tier, aggIDs, buckets)
	if err != nil {
		return nil, err
	}
	res := make([][]value.Value, len(aggIDs))
	for i := range buckets {
		res[i] = make([]value.Value, len(buckets[i]))
		for j := range buckets[i] {
			g := s.getGroup(aggIDs[i], buckets[i][j])
			index := s.getGroupIndex(buckets[i][j].Index)
			if len(vals[g][index]) == 0 {
				res[i][j] = defaults[i]
			} else {
				v, err := value.FromJSON([]byte(vals[g][index]))
				if err != nil {
					return nil, fmt.Errorf("failed to parse '%s' as value.Value", vals[g][index])
				}
				res[i][j] = v
			}
		}
	}
	s.logStats(vals, "get")
	return res, nil
}

func (s splitStore) SetMulti(
	ctx context.Context, tier tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, values [][]value.Value,
) error {
	ctx, t := timer.Start(ctx, tier.ID, "splitstore.set_multi")
	defer t.Stop()
	vals, err := s.getFromRedis(ctx, &tier, aggIDs, buckets)
	if err != nil {
		return err
	}
	// we want to set TTL only when the hashmap was empty to begin with
	setTTL := make(map[splitGroup]bool, len(vals))
	for g, l := range vals {
		setTTL[g] = true
		for i := range l {
			if len(l[i]) != 0 {
				setTTL[g] = false
				break
			}
		}
	}
	for i := range buckets {
		for j := range buckets[i] {
			g := s.getGroup(aggIDs[i], buckets[i][j])
			index := s.getGroupIndex(buckets[i][j].Index)
			if _, ok := vals[g]; !ok {
				vals[g] = make([]string, s.bucketsPerGroup)
			}
			vals[g][index] = values[i][j].String()
		}
	}
	s.logStats(vals, "set")
	keyCount := make(map[ftypes.AggId]int, len(aggIDs))
	for k := range vals {
		keyCount[k.aggID]++
	}
	for i := range aggIDs {
		tier.Logger.Info(
			"Updating redis keys for aggregate",
			zap.Int("aggregate", int(aggIDs[i])),
			zap.Int("num_keys", keyCount[aggIDs[i]]),
		)
	}
	return s.setInRedis(ctx, &tier, vals, setTTL)
}

func (s splitStore) getFromRedis(
	ctx context.Context, tier *tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket,
) (groups map[splitGroup][]string, err error) {
	groups = make(map[splitGroup][]string, len(aggIDs))
	keyCount := 0
	largestKeySize := 0
	for i := range buckets {
		for j := range buckets[i] {
			g := s.getGroup(aggIDs[i], buckets[i][j])
			if _, ok := groups[g]; !ok {
				groups[g] = make([]string, s.bucketsPerGroup)
				keyCount++
			}
			if len(g.key)+50 > largestKeySize {
				largestKeySize = len(g.key) + 50
			}
		}
	}
	keyBuf := make([]byte, largestKeySize*keyCount)
	start := 0
	rkeys := make([]string, keyCount)
	ptrs := make([]splitGroup, keyCount)
	itr := 0
	for g := range groups {
		n, err := g.getRedisKey(keyBuf, start)
		if err != nil {
			return nil, err
		}
		bkey := keyBuf[start : start+n]
		rkeys[itr] = *(*string)(unsafe.Pointer(&bkey))
		start += n
		ptrs[itr] = g
		itr++
	}
	res, err := tier.Redis.HGetAllPipelined(ctx, rkeys...)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys from redis: %w", err)
	}
	for i := range res {
		g := ptrs[i]
		for k, v := range res[i] {
			j, err := strconv.ParseUint(k, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse bucket hashmap key '%s' as int", k)
			}
			groups[g][j] = v
		}
	}
	return groups, nil
}

func (s splitStore) setInRedis(
	ctx context.Context, tier *tier.Tier, vals map[splitGroup][]string, setTTL map[splitGroup]bool,
) error {
	rkeys := make([]string, len(vals))
	rvals := make([]map[string]interface{}, len(vals))
	ttls := make([]time.Duration, len(vals))
	itr := 0
	keysSize := 0
	largestKeySize := 0
	for g := range vals {
		if len(g.key)+50 > largestKeySize {
			largestKeySize = len(g.key) + 50
		}
	}
	keyBuf := make([]byte, largestKeySize*len(vals))
	start := 0
	for g := range vals {
		v := make(map[string]interface{}, len(vals[g]))
		for j := range vals[g] {
			v[strconv.Itoa(j)] = vals[g][j]
		}
		n, err := g.getRedisKey(keyBuf, start)
		if err != nil {
			return err
		}
		bkey := keyBuf[start : start+n]
		rkeys[itr] = *(*string)(unsafe.Pointer(&bkey))
		start += n
		rvals[itr] = v
		keysSize += len(rkeys[itr])
		if setTTL[g] {
			ttls[itr] = time.Second * time.Duration(s.retention)
		}
		itr++
	}
	metrics.WithLabelValues("split_store_redis_key_size_bytes").Observe(float64(keysSize))
	return tier.Redis.HSetPipelined(ctx, rkeys, rvals, ttls)
}

// getRedisKey writes the redis key corresponding to the given aggregate id and group
// in the provided buffer at index start and returns the number of bytes written.
// This uses a buffer to avoid CPU inefficiency working with heap memory.
//
// encoding is as follows:
// 	{AggID}-{Codec}-{GroupIdentifier}
//
// where:
// 	AggrId -> base91 encoded, UvarInt serialized AggId
//	Codec  -> base91 encoded, UvarInt serialized codec
//  GroupIdentifier -> base91 encoded (NOTE: this is determined with Codec), serialization of key + pos + width
//
// we use `-` (omitted from base91 character set) as the delimiter b/w different parts of the key to prefix search for a particular
// AggId or (AggId, Codec) pair on redis
func (g splitGroup) getRedisKey(buffer []byte, start int) (int, error) {
	var aggStr, codecStr, groupStr string
	{
		aggBuf := make([]byte, 8)
		cur, err := binary.PutUvarint(aggBuf, uint64(g.aggID))
		if err != nil {
			return 0, err
		}
		e := base91.StdEncoding.EncodedLen(8)
		dest := arena.Bytes.Alloc(e, e)
		defer arena.Bytes.Free(dest)
		a, n := base91.StdEncoding.Encode(dest, aggBuf[:cur])
		if err != nil {
			return 0, err
		}
		aggStr = a[:n]
	}
	{
		codecBuf := make([]byte, 8)
		cur, err := counterCodec2.Write(codecBuf)
		if err != nil {
			return 0, err
		}
		e := base91.StdEncoding.EncodedLen(8)
		dest := arena.Bytes.Alloc(e, e)
		defer arena.Bytes.Free(dest)
		a, n := base91.StdEncoding.Encode(dest, codecBuf[:cur])
		codecStr = a[:n]
	}
	{
		sz := 8+len(g.key)+8+8
		groupBuf := make([]byte, sz)
		cur := 0
		if n, err := binary.PutString(groupBuf, g.key); err != nil {
			return 0, err
		} else {
			cur += n
		}
		if n, err := binary.PutUvarint(groupBuf[cur:], uint64(g.pos)); err != nil {
			return 0, err
		} else {
			cur += n
		}
		if n, err := binary.PutUvarint(groupBuf[cur:], uint64(g.width)); err != nil {
			return 0, err
		} else {
			cur += n
		}
		e := base91.StdEncoding.EncodedLen(sz)
		dest := arena.Bytes.Alloc(e, e)
		defer arena.Bytes.Free(dest)
		a, n := base91.StdEncoding.Encode(dest, groupBuf[:cur])
		groupStr = a[:n]
	}

	// concatenate the base91 encoded strings with `-` as the delimiter
	n := len(aggStr) + len(codecStr) + len(groupStr) + 2 // 2 bytes for delimiter
	if start+n > len(buffer) {
		return 0, fmt.Errorf("key buffer out of space")
	}
	start = writeStringToBuf(buffer, aggStr, start)
	start = writeStringToBuf(buffer, redisKeyDelimiter, start)
	start = writeStringToBuf(buffer, codecStr, start)
	start = writeStringToBuf(buffer, redisKeyDelimiter, start)
	start = writeStringToBuf(buffer, groupStr, start)
	return n, nil
}

func (s splitStore) getGroup(aggID ftypes.AggId, bucket counter.Bucket) splitGroup {
	return splitGroup{
		aggID: aggID,
		key:   bucket.Key,
		pos:   bucket.Index / s.bucketsPerGroup,
		width: bucket.Width,
	}
}

func (s splitStore) getGroupIndex(index uint32) uint32 {
	return index % s.bucketsPerGroup
}

func writeStringToBuf(buffer []byte, str string, start int) int {
	for i := range str {
		buffer[start+i] = str[i]
	}
	return start + len(str)
}
