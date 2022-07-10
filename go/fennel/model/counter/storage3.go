package counter

import (
	"context"
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
	"github.com/zeebo/xxh3"
)

var _ BucketStore = thirdStore{}

/*
	thirdStore takes buckets from a thirdBucketizer and groups these buckets into slots.
	A thirdSlot is uniquely identified by aggregate ID, its index and a prefix string.
		- A thirdSlot only contains buckets from the specified aggregate ID.
		- It only contains buckets with indices in the range: [index*bucketsPerSlot, (index+1)*bucketsPerSlot-1].
		- It only contains buckets whose groupkey's hash starts with the slot's prefix string.
	Each thirdSlot is stored as a hashmap in redis. The keys of the hashmap are the remaining part of the groupkey
	hashes (that is ignoring the prefix which is common). The value of the hashmap contains all the bucket values
	in the form of a value.Dict. The key for a bucket is obtained with (bucket.Index % bucketsPerSlot).

	NOTE: prefixSize should always be greater than 0 and less than 16.
*/
type thirdStore struct {
	bucketsPerSlot uint32
	prefixSize     int
	retention      uint64
}

func (t thirdStore) GetBucketStore() BucketStore {
	return t
}

func (t thirdStore) GetMulti(
	ctx context.Context, tier tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, defaults []value.Value,
) ([][]value.Value, error) {
	ctx, tmr := timer.Start(ctx, tier.ID, "thirdstore.get_multi")
	defer tmr.Stop()
	slots, ptrs, hashes := t.toSlots(aggIDs, buckets)
	view, err := newThirdStoreView(slots, hashes, t)
	if err != nil {
		return nil, err
	}
	err = view.Load(ctx, &tier)
	if err != nil {
		return nil, err
	}
	res := make([][]value.Value, len(buckets))
	for i := range buckets {
		res[i] = make([]value.Value, len(buckets[i]))
		for j, b := range buckets[i] {
			suffixBytes := hashes[i][j][t.prefixSize:]
			suffix := *(*string)(unsafe.Pointer(&suffixBytes))
			slot := slots[ptrs[i][j]]
			var ok bool
			res[i][j], ok = view.view[slot][suffix].Get(t.l2Index(b.Index))
			if !ok {
				res[i][j] = defaults[i]
			}
		}
	}
	return res, nil
}

func (t thirdStore) SetMulti(
	ctx context.Context, tier tier.Tier, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, values [][]value.Value,
) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "thirdstore.set_multi")
	defer tmr.Stop()
	slots, ptrs, hashes := t.toSlots(aggIDs, buckets)
	view, err := newThirdStoreView(slots, hashes, t)
	if err != nil {
		return err
	}
	err = view.Load(ctx, &tier)
	if err != nil {
		return err
	}
	for i := range buckets {
		for j, b := range buckets[i] {
			suffixBytes := hashes[i][j][t.prefixSize:]
			suffix := *(*string)(unsafe.Pointer(&suffixBytes))
			slot := slots[ptrs[i][j]]
			dict := view.view[slot][suffix]
			key := t.l2Index(b.Index)
			dict.Set(key, values[i][j])
			view.view[slot][suffix] = dict
		}
	}
	return view.Save(ctx, &tier, time.Second*time.Duration(t.retention))
}

func (t thirdStore) l2Index(bucketIndex uint32) string {
	return strconv.FormatUint(uint64(bucketIndex%t.bucketsPerSlot), 10)
}

func (t thirdStore) toSlots(aggIDs []ftypes.AggId, buckets [][]counter.Bucket) ([]thirdSlot, [][]int, [][][16]byte) {
	seen := make(map[thirdSlot]int)
	ptrs := make([][]int, len(buckets))
	hashes := make([][][16]byte, len(buckets))
	ctr := 0
	for i, aggID := range aggIDs {
		ptrs[i] = make([]int, len(buckets[i]))
		hashes[i] = make([][16]byte, len(buckets[i]))
		for j, b := range buckets[i] {
			hashes[i][j] = xxh3.HashString128(b.Key).Bytes()
			slot := thirdSlot{
				aggID:  aggID,
				index:  b.Index / t.bucketsPerSlot,
				prefix: string(hashes[i][j][:t.prefixSize]),
			}
			if _, ok := seen[slot]; !ok {
				seen[slot] = ctr
				ctr++
			}
			ptrs[i][j] = seen[slot]
		}
	}
	slots := make([]thirdSlot, len(seen))
	for slot, pos := range seen {
		slots[pos] = slot
	}
	return slots, ptrs, hashes
}

type thirdSlot struct {
	aggID  ftypes.AggId
	index  uint32
	prefix string
}

type thirdStoreView struct {
	view  map[thirdSlot]map[string]value.Dict
	keys  []string
	slots []thirdSlot
	isNew []bool
}

func newThirdStoreView(slots []thirdSlot, hashes [][][16]byte, t thirdStore) (v thirdStoreView, err error) {
	v.view = make(map[thirdSlot]map[string]value.Dict, len(slots))
	for i, slot := range slots {
		v.view[slot] = make(map[string]value.Dict, len(hashes[i]))
		for _, h := range hashes[i] {
			suffix := string(h[t.prefixSize:])
			v.view[slot][suffix] = value.NewDict(nil)
		}
	}
	v.slots = slots
	err = v.genKeys(t)
	return v, err
}

func (v *thirdStoreView) genKeys(t thirdStore) error {
	buffer := make([]byte, len(v.view)*(38+int(t.prefixSize)))
	start := 0
	v.keys = make([]string, len(v.slots))
	for i, slot := range v.slots {
		n, err := slot.redisKey(buffer, start, t.bucketsPerSlot, 16-t.prefixSize)
		if err != nil {
			return err
		}
		bkey := buffer[start : start+n]
		v.keys[i] = *(*string)(unsafe.Pointer(&bkey))
		start += n
	}
	return nil
}

func (v *thirdStoreView) Load(ctx context.Context, tier *tier.Tier) error {
	vals, err := tier.Redis.HGetAllPipelined(ctx, v.keys...)
	if err != nil {
		return err
	}
	v.isNew = make([]bool, len(vals))
	for i := range vals {
		slot := v.slots[i]
		for k := range v.view[slot] {
			valStr, ok := vals[i][k]
			if ok {
				var val value.Value
				if err := value.ProtoUnmarshal([]byte(valStr), &val); err != nil {
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

func (v *thirdStoreView) Save(ctx context.Context, tier *tier.Tier, ttl time.Duration) error {
	vals := make([]map[string]interface{}, len(v.keys))
	ttls := make([]time.Duration, len(v.keys))
	for i := range v.keys {
		slot := v.slots[i]
		vals[i] = make(map[string]interface{}, len(v.view[slot]))
		for k := range v.view[slot] {
			vser, err := value.ProtoMarshal(v.view[slot][k])
			if err != nil {
				return err
			}
			vals[i][k] = *(*string)(unsafe.Pointer(&vser))
		}
		if v.isNew[i] {
			ttls[i] = ttl
		}
	}
	return tier.Redis.HSetPipelined(ctx, v.keys, vals, ttls)
}

func (s thirdSlot) redisKey(buffer []byte, start int, bucketsPerSlot uint32, suffixSize int) (int, error) {
	length := 0

	codecStr, err := encodeCodex(counterCodec3)
	if err != nil {
		return 0, err
	}
	length += len(codecStr) + 1 // 1 extra byte for delimiter

	nums := []uint64{uint64(s.aggID), uint64(bucketsPerSlot), uint64(suffixSize)}
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
	_ = writeStringToBuf(buffer, s.prefix, start)

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
