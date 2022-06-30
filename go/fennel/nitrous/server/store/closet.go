package store

import (
	"context"
	"fmt"
	"unsafe"

	"fennel/hangar"
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/lib/utils/slice"
	"fennel/lib/value"
	"fennel/model/counter"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server/tailer"
	"fennel/nitrous/server/temporal"
	"fennel/plane"

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
	plane plane.Plane

	tierId     ftypes.RealmID
	aggId      ftypes.AggId
	codec      rpc.AggCodec
	field      []byte // field is the hangar field for this aggregate. It is created from (tier id | agg id)
	mr         counter.MergeReduce
	bucketizer temporal.TimeBucketizer
}

var _ AggregateStore = Closet{}
var _ tailer.EventProcessor = Closet{}

func NewCloset(
	plane plane.Plane, tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec,
	mr counter.MergeReduce, bucketizer temporal.TimeBucketizer) (Closet, error) {
	field, err := encodeField(tierId, aggId)
	if err != nil {
		return Closet{}, fmt.Errorf("failed to create field: %w", err)
	}
	ags := Closet{
		plane,
		tierId,
		aggId,
		codec,
		field,
		mr,
		bucketizer,
	}
	return ags, nil
}

func encodeField(tierId ftypes.RealmID, aggId ftypes.AggId) ([]byte, error) {
	buf := make([]byte, 20)
	curr := 0
	n, err := binary.PutUvarint(buf[curr:], uint64(tierId))
	if err != nil {
		return nil, fmt.Errorf("error encoding tierId (%d): %w", tierId, err)
	}
	curr += n
	n, err = binary.PutUvarint(buf[curr:], uint64(aggId))
	if err != nil {
		return nil, fmt.Errorf("error encoding aggId (%d): %w", aggId, err)
	}
	curr += n
	return buf[:curr], nil
}

func asString(s []byte) string {
	return *(*string)(unsafe.Pointer(&s))
}

func (ms Closet) encodeKeys(groupkey string, buckets []temporal.TimeBucket) ([]hangar.KeyGroup, error) {
	kgs := make([]hangar.KeyGroup, len(buckets))
	// Allocate space for storing keys.
	keylen := 10 + 10 + len(groupkey) + 10 + 10
	keybuf := make([]byte, keylen*len(buckets))
	for i, b := range buckets {
		// Encode (codec | groupkey | width | index) as "prefix".
		curr := 0
		n, err := binary.PutVarint(keybuf[curr:], int64(ms.codec))
		if err != nil {
			return nil, fmt.Errorf("error encoding codec (%d): %w", ms.codec, err)
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
		kgs[i].Prefix.Data = keybuf[:curr:curr]
		kgs[i].Fields = mo.Some(hangar.Fields{slice.Limit(ms.field)})
		keybuf = keybuf[curr:]
	}
	return kgs, nil
}

func (ms Closet) Get(ctx context.Context, duration uint32, keys []string) ([]value.Value, error) {
	buckets, err := ms.bucketizer.BucketizeDuration(duration)
	if err != nil {
		return nil, fmt.Errorf("error bucketizing: %w", err)
	}
	kgs := make([]hangar.KeyGroup, 0, len(keys)*len(buckets))
	for _, key := range keys {
		encoded, err := ms.encodeKeys(key, buckets)
		if err != nil {
			return nil, fmt.Errorf("error encoding: %w", err)
		}
		kgs = append(kgs, encoded...)
	}
	vgs, err := ms.plane.Store.GetMany(kgs)
	if err != nil {
		return nil, fmt.Errorf("error getting values: %w", err)
	}
	ret := make([]value.Value, len(keys))
	vals := make([]value.Value, len(buckets))
	for i := 0; i < len(keys); i++ {
		offset := i * len(buckets)
		for j := 0; j < len(buckets); j++ {
			vg := vgs[offset+j]
			if len(vg.Values) == 0 {
				vals[j] = ms.mr.Zero()
			} else {
				vals[j], err = value.FromJSON(vg.Values[0])
				if err != nil {
					return nil, fmt.Errorf("error decoding value(%s): %w", string(vg.Values[0]), err)
				}
			}
		}
		ret[i], err = ms.mr.Reduce(vals)
		if err != nil {
			return nil, fmt.Errorf("error reducing: %w", err)
		}
	}
	return ret, nil
}

func (ms Closet) Identity() string {
	return fmt.Sprintf("agg:%d:%d", ms.tierId, ms.aggId)
}

func (ms Closet) Process(ctx context.Context, ops []*rpc.NitrousOp) ([]hangar.Key, []hangar.ValGroup, error) {
	// TODO: Should we pre-allocate space?
	var keys []string
	var ts []uint32
	var vals []value.Value
	for _, op := range ops {
		switch op.Type {
		case rpc.OpType_AGG_EVENT:
			event := op.GetAggEvent()
			if ftypes.RealmID(op.TierId) != ms.tierId || ftypes.AggId(event.GetAggId()) != ms.aggId {
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
			val, err = ms.mr.Transform(val)
			if err != nil {
				return nil, nil, fmt.Errorf("error transforming value: %w", err)
			}
			vals = append(vals, val)
		}
	}
	return ms.Update(ctx, ts, keys, vals)
}

func (ms Closet) Update(ctx context.Context, ts []uint32, keys []string, val []value.Value) (
	[]hangar.Key, []hangar.ValGroup, error) {
	prefixes := make(map[string]int)
	var hkgs []hangar.KeyGroup
	var vals []value.Value
	var expiry []int64
	for i := 0; i < len(keys); i++ {
		buckets, ttls, err := ms.bucketizer.BucketizeMoment(ts[i])
		if err != nil {
			return nil, nil, fmt.Errorf("error bucketizing ts (%d) for aggId (%d): %w", ts[i], ms.aggId, err)
		}
		kgs, err := ms.encodeKeys(keys[i], buckets)
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
				vals[ptr], err = ms.mr.Merge(vals[ptr], val[i])
				if err != nil {
					return nil, nil, fmt.Errorf("error merging: %w", err)
				}
			}
		}
	}
	vgs, err := ms.plane.Store.GetMany(hkgs)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading from store: %w", err)
	}
	hkeys := make([]hangar.Key, len(hkgs))
	for i := range vgs {
		vg := &vgs[i]
		hkeys[i] = hkgs[i].Prefix
		if len(vg.Fields) == 0 {
			vg.Fields = append(vg.Fields, slice.Limit(ms.field))
			vg.Values = append(vg.Values, []byte(vals[i].String()))
			vg.Expiry = expiry[i]
		} else {
			if asString(vg.Fields[0]) != asString(ms.field) {
				return nil, nil, fmt.Errorf("unexpected field mismatch after select. Expected: %s, Got: %s", asString(ms.field), asString(vg.Fields[0]))
			}
			currv, err := value.FromJSON(vg.Values[0])
			if err != nil {
				return nil, nil, fmt.Errorf("error decoding value: %w", err)
			}
			vals[i], err = ms.mr.Merge(vals[i], currv)
			if err != nil {
				return nil, nil, fmt.Errorf("error merging: %w", err)
			}
			vg.Values[0] = []byte(vals[i].String())
			vg.Expiry = expiry[i]
		}
	}
	return hkeys, vgs, nil
}
