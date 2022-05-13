package counter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/tier"
	"github.com/mtraver/base91"
	"go.uber.org/zap"
)

type splitStore struct {
	bucketsPerGroup uint64
	retention       uint64
}

func (s splitStore) GetBucketStore() BucketStore {
	return splitStore{bucketsPerGroup: s.bucketsPerGroup}
}

var _ BucketStore = splitStore{}

type splitGroup struct {
	aggID ftypes.AggId
	key   string
	pos   uint64
	width uint64
}

func (s splitStore) logStats(vals map[splitGroup]map[string]value.Value, mode string) {
	valsPerKey := 0
	count := 0
	for _, v := range vals {
		valsPerKey += len(v)
		count += 1
	}
	metrics.WithLabelValues(fmt.Sprintf("l2_num_vals_per_key_in%s", mode)).Observe(float64(valsPerKey) / float64(count))
}

func (s splitStore) Get(
	ctx context.Context, tier tier.Tier, aggID ftypes.AggId, buckets []counter.Bucket, default_ value.Value,
) ([]value.Value, error) {
	defer timer.Start(ctx, tier.ID, "splitstore.get").Stop()
	res, err := s.GetMulti(ctx, tier, []ftypes.AggId{aggID}, [][]counter.Bucket{buckets}, []value.Value{default_})
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("failed to get anything")
	}
	return res[0], err
}

func (s splitStore) GetMulti(
	ctx context.Context, tier tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, defaults []value.Value,
) ([][]value.Value, error) {
	defer timer.Start(ctx, tier.ID, "splitstore.get_multi").Stop()
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
			var ok bool
			res[i][j], ok = vals[g][index]
			if !ok {
				res[i][j] = defaults[i]
			}
		}
	}
	s.logStats(vals, "get")
	return res, nil
}

func (s splitStore) Set(
	ctx context.Context, tier tier.Tier, aggID ftypes.AggId, buckets []counter.Bucket,
) error {
	defer timer.Start(ctx, tier.ID, "splitstore.set").Stop()
	return s.SetMulti(ctx, tier, []ftypes.AggId{aggID}, [][]counter.Bucket{buckets})
}

func (s splitStore) SetMulti(
	ctx context.Context, tier tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket,
) error {
	defer timer.Start(ctx, tier.ID, "splitstore.set_multi").Stop()
	vals, err := s.getFromRedis(ctx, &tier, aggIDs, buckets)
	if err != nil {
		return err
	}
	for i := range buckets {
		for j := range buckets[i] {
			g := s.getGroup(aggIDs[i], buckets[i][j])
			index := s.getGroupIndex(buckets[i][j].Index)
			if _, ok := vals[g]; !ok {
				vals[g] = make(map[string]value.Value)
			}
			vals[g][index] = buckets[i][j].Value
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
	return s.setInRedis(ctx, &tier, vals)
}

func (s splitStore) getFromRedis(
	ctx context.Context, tier *tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket,
) (groups map[splitGroup]map[string]value.Value, err error) {
	groups = make(map[splitGroup]map[string]value.Value, len(aggIDs))
	keyCount := 0
	for i := range buckets {
		for j := range buckets[i] {
			g := s.getGroup(aggIDs[i], buckets[i][j])
			if _, ok := groups[g]; !ok {
				groups[g] = make(map[string]value.Value)
				keyCount++
			}
		}
	}
	rkeys := make([]string, keyCount)
	ptrs := make([]splitGroup, keyCount)
	itr := 0
	for g := range groups {
		rkeys[itr], err = g.getRedisKey()
		if err != nil {
			return nil, err
		}
		ptrs[itr] = g
		itr++
	}
	res, err := tier.Redis.HGetAllPipelined(ctx, rkeys...)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys from redis: %w", err)
	}
	for i := range res {
		g := ptrs[i]
		v := make(map[string]value.Value, len(res[i]))
		for j := range res[i] {
			v[j], err = value.FromJSON([]byte(res[i][j]))
			if err != nil {
				return nil, fmt.Errorf("failed to parse value at field '%s' of key '%s': %w", j, rkeys[i], err)
			}
		}
		groups[g] = v
	}
	return groups, nil
}

func (s splitStore) setInRedis(
	ctx context.Context, tier *tier.Tier, vals map[splitGroup]map[string]value.Value,
) error {
	var rkeys []string
	var rvals []map[string]interface{}
	keySize := 0
	for g := range vals {
		v := make(map[string]interface{}, len(vals[g]))
		for j := range vals[g] {
			v[j] = vals[g][j].String()
		}
		k, err := g.getRedisKey()
		if err != nil {
			return err
		}
		rkeys = append(rkeys, k)
		rvals = append(rvals, v)
		keySize += len(k)
	}
	ttls := make([]time.Duration, len(rkeys))
	for i := range ttls {
		ttls[i] = time.Second * time.Duration(s.retention)
	}
	metrics.WithLabelValues("redis_key_size_bytes").Observe(float64(keySize))
	return tier.Redis.HSetPipelined(ctx, rkeys, rvals, ttls)
}

// getRedisKey returns key for an redis entry corresponding to the given aggregate id and group
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
func (g splitGroup) getRedisKey() (string, error) {
	var aggStr, codecStr, groupStr string
	{
		aggBuf := make([]byte, 8)
		cur, err := binary.PutUvarint(aggBuf, uint64(g.aggID))
		if err != nil {
			return "", err
		}
		aggStr = base91.StdEncoding.EncodeToString(aggBuf[:cur])
	}
	{
		codecBuf := make([]byte, 8)
		cur, err := counterCodec2.Write(codecBuf)
		if err != nil {
			return "", err
		}
		codecStr = base91.StdEncoding.EncodeToString(codecBuf[:cur])
	}
	{
		groupBuf := make([]byte, 8+len(g.key)+8+8)
		cur := 0
		if n, err := binary.PutString(groupBuf, g.key); err != nil {
			return "", err
		} else {
			cur += n
		}
		if n, err := binary.PutUvarint(groupBuf[cur:], g.pos); err != nil {
			return "", err
		} else {
			cur += n
		}
		if n, err := binary.PutUvarint(groupBuf[cur:], g.width); err != nil {
			return "", err
		} else {
			cur += n
		}
		groupStr = base91.StdEncoding.EncodeToString(groupBuf[:cur])
	}

	// concatenate the base91 encoded strings with `-` as the delimiter
	sb := strings.Builder{}
	sb.Grow(len(aggStr) + len(codecStr) + len(groupStr) + 2) // allocate 2 bytes for delimiter
	sb.WriteString(aggStr)
	sb.WriteString(redisKeyDelimiter)
	sb.WriteString(codecStr)
	sb.WriteString(redisKeyDelimiter)
	sb.WriteString(groupStr)
	return sb.String(), nil
}

func (s splitStore) getGroup(aggID ftypes.AggId, bucket counter.Bucket) splitGroup {
	return splitGroup{
		aggID: aggID,
		key:   bucket.Key,
		pos:   bucket.Index / s.bucketsPerGroup,
		width: bucket.Width,
	}
}

func (s splitStore) getGroupIndex(index uint64) string {
	return strconv.FormatUint(index%s.bucketsPerGroup, 10)
}
