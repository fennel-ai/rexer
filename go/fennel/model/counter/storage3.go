package counter

import (
	"context"
	"crypto/md5"
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"fennel/lib/codex"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/tier"
	"github.com/mtraver/base91"
)

var _ BucketStore = thirdStore{}

type thirdStore struct {
	l1Size     uint64
	l2Size     uint64
	hashSize   uint64
	prefixSize uint64
	retention  uint64
}

func (t thirdStore) GetBucketStore() BucketStore {
	return t
}

func (t thirdStore) Get(
	ctx context.Context, tier tier.Tier, aggID ftypes.AggId, buckets []counter.Bucket, default_ value.Value,
) ([]value.Value, error) {
	defer timer.Start(ctx, tier.ID, "thirdstore.get").Stop()
	res, err := t.GetMulti(ctx, tier, []ftypes.AggId{aggID}, [][]counter.Bucket{buckets}, []value.Value{default_})
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("failed to get anything")
	}
	return res[0], err
}

func (t thirdStore) GetMulti(
	ctx context.Context, tier tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, defaults []value.Value,
) ([][]value.Value, error) {
	defer timer.Start(ctx, tier.ID, "thirdstore.get_multi").Stop()
	reqs := t.toReqs(aggIDs, buckets)
	view, err := newThirdStoreView(reqs, t)
	if err != nil {
		return nil, err
	}
	err = view.Load(ctx, &tier)
	if err != nil {
		return nil, err
	}
	res := make([][]value.Value, len(buckets))
	for i, aggID := range aggIDs {
		res[i] = make([]value.Value, len(buckets[i]))
		for j, b := range buckets[i] {
			hash := md5.Sum([]byte(b.Key))
			slot := l1Slot{
				aggID:  aggID,
				index:  (b.Index * t.l2Size) / t.l1Size,
				prefix: string(hash[:t.prefixSize]),
			}
			var ok bool
			res[i][j], ok = view.view[slot][string(hash[t.prefixSize:])].Get(strconv.Itoa(int(b.Index % (t.l2Size / t.l1Size))))
			if !ok {
				res[i][j] = defaults[i]
			}
		}
	}
	return res, nil
}

func (t thirdStore) Set(
	ctx context.Context, tier tier.Tier, aggID ftypes.AggId, buckets []counter.Bucket,
) error {
	defer timer.Start(ctx, tier.ID, "thirdstore.set").Stop()
	return t.SetMulti(ctx, tier, []ftypes.AggId{aggID}, [][]counter.Bucket{buckets})
}

func (t thirdStore) SetMulti(
	ctx context.Context, tier tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket,
) error {
	defer timer.Start(ctx, tier.ID, "thirdstore.set_multi").Stop()
	reqs := t.toReqs(aggIDs, buckets)
	view, err := newThirdStoreView(reqs, t)
	if err != nil {
		return err
	}
	err = view.Load(ctx, &tier)
	if err != nil {
		return err
	}
	for i, aggID := range aggIDs {
		for _, b := range buckets[i] {
			hash := md5.Sum([]byte(b.Key))
			slot := l1Slot{
				aggID:  aggID,
				index:  (b.Index * t.l2Size) / t.l1Size,
				prefix: string(hash[:t.prefixSize]),
			}
			dict := view.view[slot][string(hash[t.prefixSize:])]
			dict.Set(strconv.Itoa(int(b.Index%(t.l2Size/t.l1Size))), b.Value)
			view.view[slot][string(hash[t.prefixSize:])] = dict
		}
	}
	return view.Save(ctx, &tier, time.Second*time.Duration(t.retention))
}

func (t thirdStore) toReqs(aggIDs []ftypes.AggId, buckets [][]counter.Bucket) map[l1Slot]map[string]bool {
	reqs := make(map[l1Slot]map[string]bool)
	for i, aggID := range aggIDs {
		for _, b := range buckets[i] {
			hash := md5.Sum([]byte(b.Key))
			slot := l1Slot{
				aggID:  aggID,
				index:  (b.Index * t.l2Size) / t.l1Size,
				prefix: string(hash[:t.prefixSize]),
			}
			reqs[slot][string(hash[t.prefixSize:])] = true
		}
	}
	return reqs
}

type l1Slot struct {
	aggID  ftypes.AggId
	index  uint64
	prefix string
}

type thirdStoreView struct {
	view  map[l1Slot]map[string]value.Dict
	keys  []string
	ptrs  []l1Slot
	isNew []bool
}

func newThirdStoreView(reqs map[l1Slot]map[string]bool, t thirdStore) (v thirdStoreView, err error) {
	v.view = make(map[l1Slot]map[string]value.Dict, len(reqs))
	for slot, smap := range reqs {
		v.view[slot] = make(map[string]value.Dict, len(smap))
		for suffix := range smap {
			v.view[slot][suffix] = value.NewDict(nil)
		}
	}
	err = v.genKeys(t)
	return v, err
}

func (v thirdStoreView) genKeys(t thirdStore) error {
	buffer := make([]byte, len(v.view)*(38+int(t.prefixSize)))
	start := 0
	itr := 0
	for slot := range v.view {
		n, err := slot.redisKey(buffer, start, t.l1Size, t.l2Size, t.hashSize-t.prefixSize)
		if err != nil {
			return err
		}
		bkey := buffer[start : start+n]
		v.keys[itr] = *(*string)(unsafe.Pointer(&bkey))
		start += n
		v.ptrs[itr] = slot
		itr++
	}
	return nil
}

func (v thirdStoreView) Load(ctx context.Context, tier *tier.Tier) error {
	vals, err := tier.Redis.HGetAllPipelined(ctx, v.keys...)
	if err != nil {
		return err
	}
	for i := range vals {
		slot := v.ptrs[i]
		for k := range v.view[slot] {
			valStr, ok := vals[i][k]
			if ok {
				var val value.Value
				if err := value.Unmarshal([]byte(valStr), &val); err != nil {
					return fmt.Errorf("failed to unmarshal '%s' into value", valStr)
				}
				if v.view[slot][k], ok = val.(value.Dict); !ok {
					return fmt.Errorf("expected value to be dict but found: '%s'", val.String())
				}
			}
		}
		if len(vals[i]) == 0 {
			v.isNew[i] = true
		}
	}
	return nil
}

func (v thirdStoreView) Save(ctx context.Context, tier *tier.Tier, ttl time.Duration) error {
	vals := make([]map[string]interface{}, len(v.keys))
	ttls := make([]time.Duration, len(v.keys))
	for i := range v.keys {
		slot := v.ptrs[i]
		for k := range v.view[slot] {
			vals[i][k] = v.view[slot][k]
		}
		if v.isNew[i] {
			ttls[i] = ttl
		}
	}
	return tier.Redis.HSetPipelined(ctx, v.keys, vals, ttls)
}

func (s l1Slot) redisKey(buffer []byte, start int, l1Size, l2Size, suffixSize uint64) (int, error) {
	length := 0

	codecStr, err := encodeCodex(counterCodec3)
	if err != nil {
		return 0, err
	}
	length += len(codecStr) + 1 // 1 extra byte for delimiter

	nums := []uint64{uint64(s.aggID), l1Size, l2Size, suffixSize}
	words := make([]string, len(nums))
	for i := range nums {
		words[i], err = encodeUint64(nums[i])
		if err != nil {
			return 0, err
		}
		length += len(words[i]) + 1 // 1 extra byte for delimiter
	}

	length += len(s.prefix)

	if start+length > len(buffer) {
		return 0, fmt.Errorf("key buffer out of space")
	}
	start = writeStringToBuf(buffer, codecStr, start)
	start = writeStringToBuf(buffer, redisKeyDelimiter, start)
	for i := range words {
		start = writeStringToBuf(buffer, words[i], start)
		start = writeStringToBuf(buffer, redisKeyDelimiter, start)
	}
	start = writeStringToBuf(buffer, s.prefix, start)

	return length, nil
}

func encodeUint64(v uint64) (string, error) {
	buf := make([]byte, 8)
	cur, err := binary.PutUvarint(buf, v)
	if err != nil {
		return "", err
	}
	return toBase91(buf[:cur]), nil
}

func encodeCodex(c codex.Codex) (string, error) {
	buf := make([]byte, 8)
	cur, err := c.Write(buf)
	if err != nil {
		return "", err
	}
	return toBase91(buf[:cur]), nil
}

func toBase91(b []byte) string {
	return base91.StdEncoding.EncodeToString(b)
}
