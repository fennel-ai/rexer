package store

import (
	"context"
	"fmt"
	"reflect"
	"unsafe"

	"fennel/hangar"
	"fennel/lib/aggregate"
	"fennel/lib/arena"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server/temporal"

	"github.com/samber/lo"
	"github.com/samber/mo"
	"google.golang.org/protobuf/encoding/protojson"
)

// Closet encodes aggregate data as a two-level hierarchy in hangar.
// The hangar key is (tierId | codec | groupkey | width | first-level index), and
// the hangar field is (aggId | second-level index).
// The first-level index is decided by the bucket index and the size of the
// second-level index. For example, if the size of the second-level index is 25,
// buckets in the range [0, 25) are stored under the first-level index 0,
// buckets in the range [25, 50) are stored under the first-level index 1, and
// so on. The second-level index is computed as the bucket index modulo the size
// of the second-level index. In addition, each hangar key also has a field that
// stores the pre-aggregated values for all the fields in that key. This field
// is simply identified by the encoded aggregate Id.
// This key design ensures that all aggregate values for a groupkey in the same
// bucket are colocated in the same hangar key, giving us high block-cache hit ratio,
// since they are likely to be accessed together.
type Closet struct {
	tierId ftypes.RealmID
	aggId  ftypes.AggId
	codec  rpc.AggCodec

	mr              counter.MergeReduce
	bucketizer      temporal.TimeBucketizer
	secondLevelSize int
}

func NewCloset(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec,
	mr counter.MergeReduce, bucketizer temporal.TimeBucketizer, secondLevelSize int) (*Closet, error) {
	ags := Closet{
		tierId,
		aggId,
		codec,
		mr,
		bucketizer,
		secondLevelSize,
	}
	return &ags, nil
}

func asString(s []byte) string {
	return *(*string)(unsafe.Pointer(&s))
}

func unsafeGetBytes(s string) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data)), len(s))
}

// Encode (tierId | codec | groupkey | width | first-level index) as hangar key.
func (c *Closet) encodeKey(keybuf []byte, groupkey string, width uint32, firstLevelIdx int) (int, error) {
	curr := 0
	n, err := binary.PutUvarint(keybuf[curr:], uint64(c.tierId))
	if err != nil {
		return 0, fmt.Errorf("error encoding tierId (%d): %w", c.tierId, err)
	}
	curr += n
	n, err = binary.PutVarint(keybuf[curr:], int64(c.codec))
	if err != nil {
		return 0, fmt.Errorf("error encoding codec (%d): %w", c.codec, err)
	}
	curr += n
	n, err = binary.PutString(keybuf[curr:], groupkey)
	if err != nil {
		return 0, fmt.Errorf("error encoding groupkey (%s): %w", groupkey, err)
	}
	curr += n
	n, err = binary.PutUvarint(keybuf[curr:], uint64(width))
	if err != nil {
		return 0, fmt.Errorf("error encoding width (%d): %w", width, err)
	}
	curr += n
	n, err = binary.PutUvarint(keybuf[curr:], uint64(firstLevelIdx))
	if err != nil {
		return 0, fmt.Errorf("error encoding index (%d): %w", firstLevelIdx, err)
	}
	curr += n
	return curr, nil
}

// Encode (aggId | second-level index) as hangar field if second-level index is
// present. Otherwise, just encode aggId as hangar field.
func encodeField(buf []byte, aggId ftypes.AggId, secondLevelIdx mo.Option[int]) (int, error) {
	curr := 0
	n, err := binary.PutUvarint(buf[curr:], uint64(aggId))
	if err != nil {
		return 0, fmt.Errorf("error encoding aggId (%d): %w", aggId, err)
	}
	curr += n
	if secondLevelIdx.IsPresent() {
		n, err = binary.PutUvarint(buf[curr:], uint64(secondLevelIdx.MustGet()))
		if err != nil {
			return 0, fmt.Errorf("error encoding second-level index (%d): %w", secondLevelIdx.MustGet(), err)
		}
		curr += n
	}
	return curr, nil
}

// Given the groupkey and the buckets that have new values, get the keygroups
// that need to be updated.
func (c *Closet) getKeyGroupsToUpdate(groupkey string, buckets []temporal.TimeBucket) ([]hangar.KeyGroup, error) {
	// Allocate space for storing keys.
	keyLen := 10 + 10 + len(groupkey) + 10 + 10
	fieldLen := 10 + 10
	// Make enough space for 2 fields per key.
	// Note: We don't allocate this byte slice using arena since the keys and
	// fields in the returned KeyGroups point to locations in this byte slice.
	buf := make([]byte, (keyLen+2*fieldLen)*len(buckets))
	// Encoded keys are stored in kgs, but the key prefix points to locations in
	// the keybuf slice.
	kgs := make([]hangar.KeyGroup, len(buckets))
	for i, b := range buckets {
		curr := 0

		// Encode key.
		n, err := c.encodeKey(buf[curr:], groupkey, b.Width, int(b.Index)/c.secondLevelSize)
		if err != nil {
			return nil, fmt.Errorf("error encoding key: %w", err)
		}
		key := buf[curr : curr+n : curr+n]
		curr += n

		// Encode bucket-specific fields.
		n, err = encodeField(buf[curr:], c.aggId, mo.Some(int(b.Index)%c.secondLevelSize))
		if err != nil {
			return nil, fmt.Errorf("error encoding field: %w", err)
		}
		field := buf[curr : curr+n : curr+n]
		curr += n

		// Encode field for pre-aggregated values.
		n, err = encodeField(buf[curr:], c.aggId, mo.None[int]())
		if err != nil {
			return nil, fmt.Errorf("error encoding field: %w", err)
		}
		preAggField := buf[curr : curr+n : curr+n]
		curr += n

		kgs[i].Prefix.Data = key
		kgs[i].Fields = mo.Some(hangar.Fields{field, preAggField})
		buf = buf[curr:]
	}
	return kgs, nil
}

// Given a groupkey and the range of buckets that need to be aggregated, get
// the hangar keygroups to read. This function leverages the two-level storage
// structure to only read the pre-aggregated field for keys for which all fields
// need to be aggregated.
func (c *Closet) getKeyGroupsToRead(groupkey string, r temporal.TimeBucketRange) ([]hangar.KeyGroup, error) {
	// Allocate space for storing keys.
	keyLen := 10 + 10 + len(groupkey) + 10 + 10
	fieldLen := 10 + 10
	// This is the maximum number of keys that we will need to read from hangar.
	numKeys := int(r.EndIdx-r.StartIdx)/c.secondLevelSize + 2
	// The number of fields should be less than numKeys + c.secondLevelSize
	// since all but c.secondLevelSize buckets should be computed via
	// pre-aggregated fields.
	numFields := c.secondLevelSize + numKeys
	// Note: We don't allocate this byte slice using arena since the keys and
	// fields in the returned KeyGroups point to locations in this byte slice.
	buf := make([]byte, (keyLen*numKeys)+(numFields*fieldLen))
	next := int(r.StartIdx)
	last := int(r.EndIdx)
	kgs := make([]hangar.KeyGroup, 0, numKeys)
	for next <= last {
		firstLevelIdx := next / c.secondLevelSize
		// Encode key.
		curr := 0
		n, err := c.encodeKey(buf[curr:], groupkey, r.Width, firstLevelIdx)
		if err != nil {
			return nil, fmt.Errorf("error encoding key: %w", err)
		}
		key := hangar.Key{Data: buf[curr : curr+n : curr+n]}
		curr += n
		// Get the field(s) to read for this key.
		// If all the fields are in range, just use the field containing
		// pre-aggregated values. Otherwise, add all the required fields
		// individually to the keygroup.
		if (next%c.secondLevelSize) == 0 && next+c.secondLevelSize <= last {
			// Include the pre-computed field for this second-level bucket
			n, err := encodeField(buf[curr:], c.aggId, mo.None[int]())
			if err != nil {
				return nil, fmt.Errorf("error encoding field: %w", err)
			}
			field := buf[curr : curr+n : curr+n]
			curr += n
			kgs = append(kgs, hangar.KeyGroup{
				Prefix: key,
				Fields: mo.Some(hangar.Fields{field}),
			})
			next += c.secondLevelSize
		} else {
			end := (firstLevelIdx+1)*c.secondLevelSize - 1
			if end > last {
				end = last
			}
			fields := make(hangar.Fields, 0, end-next+1)
			for i := next; i <= end; i++ {
				// encode i-th bucket.
				n, err := encodeField(buf[curr:], c.aggId, mo.Some(i%c.secondLevelSize))
				if err != nil {
					return nil, fmt.Errorf("error encoding field: %w", err)
				}
				field := buf[curr : curr+n : curr+n]
				curr += n
				fields = append(fields, field)
			}
			kgs = append(kgs, hangar.KeyGroup{
				Prefix: key,
				Fields: mo.Some(fields),
			})
			next = end + 1
		}
		buf = buf[curr:]
	}
	return kgs, nil
}

func (c *Closet) Options() aggregate.Options {
	return c.mr.Options()
}

func (c *Closet) Get(ctx context.Context, keys []string, kwargs []value.Dict, store hangar.Hangar) ([]value.Value, error) {
	ctx, t := timer.Start(ctx, c.tierId, "nitrous.closet.get")
	defer t.Stop()
	kgs := make([]hangar.KeyGroup, 0, len(keys)*(c.bucketizer.NumBucketsHint()/c.secondLevelSize+2))
	// Slice containing number of buckets for each key. This is useful in
	// dividing up the read result from hangar.
	numKeys := arena.Ints.Alloc(len(keys), len(keys))
	defer arena.Ints.Free(numKeys)
	var maxFields int
	for i, key := range keys {
		duration, err := getRequestDuration(c.mr.Options(), kwargs[i])
		if err != nil {
			return nil, fmt.Errorf("error extracting duration from request: %w", err)
		}
		buckets, err := c.bucketizer.Bucketize(c.mr, duration)
		if err != nil {
			return nil, fmt.Errorf("error bucketizing: %w", err)
		}
		encoded, err := c.getKeyGroupsToRead(key, buckets)
		if err != nil {
			return nil, fmt.Errorf("error encoding: %w", err)
		}
		kgs = append(kgs, encoded...)
		numKeys[i] = len(encoded)
		numFields := 0
		for _, kg := range encoded {
			if kg.Fields.IsAbsent() {
				numFields += c.levelSize
			} else {
				numFields += len(kg.Fields.OrEmpty())
			}
		}
		if numFields > maxFields {
			maxFields = numFields
		}
	}
	vgs, err := store.GetMany(ctx, kgs)
	if err != nil {
		return nil, fmt.Errorf("error getting values: %w", err)
	}
	ret := make([]value.Value, len(keys))
	// Slice allocated for storing the result of each field.
	vals := arena.Values.Alloc(maxFields, maxFields)
	defer arena.Values.Free(vals)
	for i := 0; i < len(keys); i++ {
		n := numKeys[i]
		intermediate := vals[:0]
		for _, vg := range vgs[:n] {
			for _, v := range vg.Values {
				uv, err := value.Unmarshal(v)
				if err != nil {
					return nil, fmt.Errorf("error decoding value(%s): %w", uv, err)
				}
				intermediate = append(intermediate, uv)
			}
		}
		ret[i], err = c.mr.Reduce(intermediate)
		if err != nil {
			return nil, fmt.Errorf("error reducing: %w", err)
		}
		vgs = vgs[n:]
	}
	return ret, nil
}

func (c *Closet) Identity() string {
	return fmt.Sprintf("agg:%d:%d", c.tierId, c.aggId)
}

func (c *Closet) Process(ctx context.Context, ops []*rpc.NitrousOp, store hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
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
			vals = append(vals, val)
		}
	}
	return c.update(ctx, ts, keys, vals, store)
}

// The update executes in 4 phases:
// 1. Decide which keygroups to update and what values to update them with.
// 2. Fetch the current value for these keygroups.
// 3. Merge the existing values with the new values.
// 4. Return the updated keygroups and values.
func (c *Closet) update(ctx context.Context, ts []uint32, groupkeys []string, val []value.Value, store hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	// Phase 1: Decide which keygroups to update and what values to update them with.

	// type `fieldUpdates` contains the list of all fields to update and what
	// values to update them with. We associate once `fieldUpdates` value with
	// each unique hangar key.
	type fieldUpdates struct {
		fields hangar.Fields
		vals   []value.Value
	}
	var updates []fieldUpdates
	// prefixes is the map of all unique hangar keys to the index of their `fieldUpdates`.
	prefixes := make(map[string]int)
	// Expiry contains the desired expiry time for each updated hangar key. The
	// actual expiry time is only updated if this is greater than the current
	// expiry time.
	var expiry []int64
	// We iterate over each groupkey and identify the keygroups to update.
	for i := 0; i < len(groupkeys); i++ {
		v, err := c.mr.Transform(val[i])
		if err != nil {
			return nil, nil, fmt.Errorf("error transforming value (%s): %w", val[i], err)
		}
		buckets, ttls, err := c.bucketizer.BucketizeMoment(c.mr, ts[i])
		if err != nil {
			return nil, nil, fmt.Errorf("error bucketizing ts (%d) for aggId (%d): %w", ts[i], c.aggId, err)
		}
		kgs, err := c.getKeyGroupsToUpdate(groupkeys[i], buckets)
		if err != nil {
			return nil, nil, fmt.Errorf("error encoding buckets: %w", err)
		}
		// Now that we have the keygroups we need to update for this groupkey,
		// we try to merge the updates with updates to the same keygroup(s) from
		// previous groupkeys.
		for j, kg := range kgs {
			p := asString(kg.Prefix.Data)
			if ptr, ok := prefixes[p]; !ok {
				prefixes[p] = len(updates)
				updates = append(updates, fieldUpdates{
					fields: kg.Fields.OrEmpty(),
					vals:   lo.Repeat(len(kg.Fields.OrEmpty()), v),
				})
				expiry = append(expiry, ttls[j])
			} else {
				present := make(map[string]int, len(updates[ptr].fields))
				for i, f := range updates[ptr].fields {
					present[asString(f)] = i
				}
				update := &updates[ptr]
				for _, f := range kg.Fields.OrEmpty() {
					if fieldIdx, ok := present[asString(f)]; !ok {
						update.fields = append(update.fields, f)
						update.vals = append(update.vals, v)
					} else {
						update.vals[fieldIdx], err = c.mr.Merge(update.vals[fieldIdx], v)
						if err != nil {
							return nil, nil, fmt.Errorf("error merging: %w", err)
						}
					}
				}
				if ttls[j] > expiry[ptr] {
					expiry[ptr] = ttls[j]
				}
			}
		}
	}
	// Create the final list of all keygroups to update.
	updatedKgs := make([]hangar.KeyGroup, len(prefixes))
	// We find the count of all fields that need to be updated. This is an
	// optimization to allow us to allocate a single slice to hold all the
	// final values.
	fieldCount := 0
	for p, idx := range prefixes {
		updatedKgs[idx].Prefix = hangar.Key{Data: unsafeGetBytes(p)}
		updatedKgs[idx].Fields = mo.Some(updates[idx].fields)
		fieldCount += len(updates[idx].fields)
	}
	// Phase 2: Fetch the current value for these keygroups.
	vgs, err := store.GetMany(ctx, updatedKgs)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading from store: %w", err)
	}
	// Phase 3: Merge the existing values with the new values.
	updatedKeys := make([]hangar.Key, len(updatedKgs))
	updatedValues := make(hangar.Values, fieldCount)
	for i := range vgs {
		updatedKeys[i] = updatedKgs[i].Prefix
		vg := &vgs[i]
		curr := make(map[string]value.Value, len(vg.Fields))
		for j, f := range vg.Fields {
			currv, err := value.Unmarshal(vg.Values[j])
			if err != nil {
				return nil, nil, fmt.Errorf("error decoding value: %w", err)
			}
			curr[asString(f)] = currv
		}
		update := updates[i]
		vg.Fields = update.fields
		vg.Values = updatedValues[:len(vg.Fields):len(vg.Fields)]
		updatedValues = updatedValues[len(vg.Fields):]
		for j, f := range update.fields {
			if currv, ok := curr[asString(f)]; ok {
				update.vals[j], err = c.mr.Merge(update.vals[j], currv)
				if err != nil {
					return nil, nil, fmt.Errorf("error merging: %w", err)
				}
			}
			vg.Values[j], err = value.Marshal(update.vals[j])
			if err != nil {
				return nil, nil, fmt.Errorf("error marshaling value: %w", err)
			}
		}
		if expiry[i] > vg.Expiry {
			vg.Expiry = expiry[i]
		}
	}
	// Phase 4: Return the updated keygroups and values.
	return updatedKeys, vgs, nil
}
