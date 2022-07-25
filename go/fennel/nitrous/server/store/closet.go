package store

import (
	"context"
	"fmt"
	"unsafe"

	"fennel/hangar"
	"fennel/lib/aggregate"
	"fennel/lib/arena"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/lib/utils/slice"
	"fennel/lib/value"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server/temporal"

	"github.com/samber/mo"
	"google.golang.org/protobuf/encoding/protojson"
)

// Closet stores aggregate data as fields in a hangar key that is created from
// the codec, groupkey, and the bucket width and index determined by the given
// bucketizer.
// This ensures that all aggregate values for a groupkey in the same bucket
// are colocated in the same hangar key, giving us high block-cache hit ratio,
// since they are likely to be accessed together.
type Closet struct {
	tierId     ftypes.RealmID
	aggId      ftypes.AggId
	codec      rpc.AggCodec
	field      []byte // field is the hangar field for this aggregate. It is created from (tier id | agg id)
	mr         counter.MergeReduce
	bucketizer temporal.TimeBucketizer
}

func NewCloset(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec,
	mr counter.MergeReduce, bucketizer temporal.TimeBucketizer) (*Closet, error) {
	field, err := encodeField(aggId)
	if err != nil {
		return nil, fmt.Errorf("failed to create field: %w", err)
	}
	ags := Closet{
		tierId,
		aggId,
		codec,
		field,
		mr,
		bucketizer,
	}
	return &ags, nil
}

func encodeField(aggId ftypes.AggId) ([]byte, error) {
	buf := make([]byte, 20)
	curr := 0
	n, err := binary.PutUvarint(buf[curr:], uint64(aggId))
	if err != nil {
		return nil, fmt.Errorf("error encoding aggId (%d): %w", aggId, err)
	}
	curr += n
	return buf[:curr], nil
}

func asString(s []byte) string {
	return *(*string)(unsafe.Pointer(&s))
}

func (c *Closet) encodeKeys(groupkey string, buckets []temporal.TimeBucket) ([]hangar.KeyGroup, error) {
	kgs := make([]hangar.KeyGroup, len(buckets))
	// Allocate space for storing keys.
	keylen := 10 + 10 + len(groupkey) + 10 + 10
	keybuf := make([]byte, keylen*len(buckets))
	for i, b := range buckets {
		// Encode (codec | groupkey | width | index) as "prefix".
		curr := 0
		n, err := binary.PutVarint(keybuf[curr:], int64(c.codec))
		if err != nil {
			return nil, fmt.Errorf("error encoding codec (%d): %w", c.codec, err)
		}
		curr += n
		n, err = binary.PutString(keybuf[curr:], groupkey)
		if err != nil {
			return nil, fmt.Errorf("error encoding groupkey (%s): %w", groupkey, err)
		}
		curr += n
		n, err = binary.PutUvarint(keybuf[curr:], uint64(b.Width))
		if err != nil {
			return nil, fmt.Errorf("error encoding width (%d): %w", b.Width, err)
		}
		curr += n
		n, err = binary.PutUvarint(keybuf[curr:], uint64(b.Index))
		if err != nil {
			return nil, fmt.Errorf("error encoding index (%d): %w", b.Index, err)
		}
		curr += n
		n, err = binary.PutUvarint(keybuf[curr:], uint64(c.tierId))
		if err != nil {
			return nil, fmt.Errorf("error encoding tierId (%d): %w", c.tierId, err)
		}
		curr += n
		kgs[i].Prefix.Data = keybuf[:curr:curr]
		kgs[i].Fields = mo.Some(hangar.Fields{slice.Limit(c.field)})
		keybuf = keybuf[curr:]
	}
	return kgs, nil
}

func (c *Closet) Options() aggregate.Options {
	return c.mr.Options()
}

func (c *Closet) Get(ctx context.Context, keys []string, kwargs []value.Dict, store hangar.Hangar) ([]value.Value, error) {
	kgs := make([]hangar.KeyGroup, 0, len(keys)*(c.bucketizer.NumBucketsHint()+1))
	// Slice containing number of buckets for each key. This is useful in
	// dividing up the read result from hangar.
	bucketLengths := make([]int, len(keys))
	for i, key := range keys {
		duration, err := getRequestDuration(c.mr.Options(), kwargs[i])
		if err != nil {
			return nil, fmt.Errorf("error extracting duration from request: %w", err)
		}
		buckets, err := c.bucketizer.Bucketize(c.mr, duration)
		if err != nil {
			return nil, fmt.Errorf("error bucketizing: %w", err)
		}
		encoded, err := c.encodeKeys(key, buckets)
		if err != nil {
			return nil, fmt.Errorf("error encoding: %w", err)
		}
		kgs = append(kgs, encoded...)
		bucketLengths[i] = len(buckets)
	}
	vgs, err := store.GetMany(ctx, kgs)
	if err != nil {
		return nil, fmt.Errorf("error getting values: %w", err)
	}
	ret := make([]value.Value, len(keys))
	offset := 0
	for i := 0; i < len(keys); i++ {
		numBuckets := bucketLengths[i]
		vals := arena.Values.Alloc(numBuckets, numBuckets)
		defer arena.Values.Free(vals)
		for j := 0; j < numBuckets; j++ {
			vg := vgs[offset+j]
			if len(vg.Values) == 0 {
				vals[j] = c.mr.Zero()
			} else {
				var err error
				vals[j], err = value.Unmarshal(vg.Values[0])
				if err != nil {
					return nil, fmt.Errorf("error decoding value(%s): %w", string(vg.Values[0]), err)
				}
			}
		}
		offset += numBuckets
		ret[i], err = c.mr.Reduce(vals)
		if err != nil {
			return nil, fmt.Errorf("error reducing: %w", err)
		}
	}
	return ret, nil
}

func (c *Closet) Identity() string {
	return fmt.Sprintf("agg:%d:%d", c.tierId, c.aggId)
}

func (c *Closet) Process(ctx context.Context, ops []*rpc.NitrousOp, store hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	// TODO: Should we pre-allocate space?
	var keys []string
	var ts []uint32
	var vals []value.Value
	for _, op := range ops {
		switch op.Type {
		case rpc.OpType_AGG_EVENT:
			event := op.GetAggEvent()
			if ftypes.RealmID(op.TierId) != c.tierId || ftypes.AggId(event.GetAggId()) != c.aggId {
				continue
			}
			keys = append(keys, event.GetGroupkey())
			ts = append(ts, event.GetTimestamp())
			pvalue := event.GetValue()
			val, err := value.FromProtoValue(pvalue)
			if err != nil {
				s, err := protojson.Marshal(pvalue)
				if err != nil {
					return nil, nil, fmt.Errorf("error decoding and marshaling value: %w", err)
				} else {
					return nil, nil, fmt.Errorf("error decoding value %s: %w", s, err)
				}
			}
			val, err = c.mr.Transform(val)
			if err != nil {
				return nil, nil, fmt.Errorf("error transforming value: %w", err)
			}
			vals = append(vals, val)
		}
	}
	return c.update(ctx, ts, keys, vals, store)
}

func (c *Closet) update(ctx context.Context, ts []uint32, keys []string, val []value.Value, store hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	prefixes := make(map[string]int)
	var hkgs []hangar.KeyGroup
	var vals []value.Value
	var expiry []int64
	for i := 0; i < len(keys); i++ {
		buckets, ttls, err := c.bucketizer.BucketizeMoment(c.mr, ts[i])
		if err != nil {
			return nil, nil, fmt.Errorf("error bucketizing ts (%d) for aggId (%d): %w", ts[i], c.aggId, err)
		}
		kgs, err := c.encodeKeys(keys[i], buckets)
		if err != nil {
			return nil, nil, fmt.Errorf("error encoding buckets: %w", err)
		}
		for j, kg := range kgs {
			p := asString(kg.Prefix.Data)
			if ptr, ok := prefixes[p]; !ok {
				prefixes[p] = len(vals)
				vals = append(vals, val[i])
				expiry = append(expiry, ttls[j])
				hkgs = append(hkgs, kg)
			} else {
				vals[ptr], err = c.mr.Merge(vals[ptr], val[i])
				if err != nil {
					return nil, nil, fmt.Errorf("error merging: %w", err)
				}
			}
		}
	}
	vgs, err := store.GetMany(ctx, hkgs)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading from store: %w", err)
	}
	hkeys := make([]hangar.Key, len(hkgs))
	for i := range vgs {
		vg := &vgs[i]
		hkeys[i] = hkgs[i].Prefix
		if len(vg.Fields) == 0 {
			v, err := value.Marshal(vals[i])
			if err != nil {
				return nil, nil, fmt.Errorf("error marshaling value: %w", err)
			}
			vg.Fields = append(vg.Fields, slice.Limit(c.field))
			vg.Values = append(vg.Values, v)
			vg.Expiry = expiry[i]
		} else {
			if asString(vg.Fields[0]) != asString(c.field) {
				return nil, nil, fmt.Errorf("unexpected field mismatch after select. Expected: %s, Got: %s", asString(c.field), asString(vg.Fields[0]))
			}
			currv, err := value.Unmarshal(vg.Values[0])
			if err != nil {
				return nil, nil, fmt.Errorf("error decoding value: %w", err)
			}
			vals[i], err = c.mr.Merge(vals[i], currv)
			if err != nil {
				return nil, nil, fmt.Errorf("error merging: %w", err)
			}
			vg.Values[0], err = value.Marshal(vals[i])
			if err != nil {
				return nil, nil, fmt.Errorf("error marshaling value: %w", err)
			}
			vg.Expiry = expiry[i]
		}
	}
	return hkeys, vgs, nil
}
