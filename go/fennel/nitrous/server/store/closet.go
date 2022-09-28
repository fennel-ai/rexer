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

// Closet encodes aggregate data as two-level hierarchies in hangar.
// The second-level key stores the first-level keys as fields, where each field
// stores the aggregated value in the smallest time bucket (as determined by
// the bucketizer).
// The third-level key stores the second-level keys as fields, where each field
// stores the pre-aggregated values for fields in the corresponding second-level
// key.
//
// The hangar key for second-level keys is:
// (tierId | codec | groupkey | width | mod | aggId | second-level index),
// and the hangar field is (first-level index).
// The second-level index is decided by the bucket index and the size of the
// first level. For example, if the size of the first level is 25, buckets in
// the range [0, 25) are stored under the second-level index 0, buckets in the
// range [25, 50) are stored under the second-level index 1, and so on.
// The first-level index is computed as the bucket index modulo the size
// of the first level.
// The "mod" field is present to partition the keyspace into chunks that are
// frequently read together -- it is computed as the second-level index
// modulo 'k', where k is the number of buckets created by the bucketizer (see
// temporal.TimeBucketizer.NumBucketsHint) divided by level size.
// (e.g. if num buckets = 100 and level size 25, then k = 100 / 25 = 4).
//
// The hangar key for third-level keys data is:
// (tierId | codec | groupkey | width | aggId | third-level index),
// and the hangar field is (second-level index).
type Closet struct {
	tierId ftypes.RealmID
	aggId  ftypes.AggId
	codec  rpc.AggCodec

	mr         counter.MergeReduce
	bucketizer temporal.TimeBucketizer
	levelSize  int
}

func NewCloset(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec,
	mr counter.MergeReduce, bucketizer temporal.TimeBucketizer, levelSize int) (*Closet, error) {
	ags := Closet{
		tierId,
		aggId,
		codec,
		mr,
		bucketizer,
		levelSize,
	}
	return &ags, nil
}

func asString(s []byte) string {
	return *(*string)(unsafe.Pointer(&s))
}

func unsafeGetBytes(s string) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data)), len(s))
}

func (c *Closet) getKeyLenHint(gk string) int {
	return len(gk) + 60 /* 6 varint fields */
}

func (c *Closet) encodeThirdLevelKey(keybuf []byte, groupkey string, width uint32, thirdLevelIdx int) (int, error) {
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
	n, err = binary.PutUvarint(keybuf[curr:], uint64(c.aggId))
	if err != nil {
		return 0, fmt.Errorf("error encoding agg Id (%d): %w", c.aggId, err)
	}
	curr += n
	n, err = binary.PutUvarint(keybuf[curr:], uint64(thirdLevelIdx))
	if err != nil {
		return 0, fmt.Errorf("error encoding index (%d): %w", thirdLevelIdx, err)
	}
	curr += n
	return curr, nil
}

// Encode (tierId | codec | groupkey | width | mod | aggId | second-level index) as hangar key.
// At any given moment and given an aggregate width, we will only be looking at
// keys with a particular value of "mod" for all aggregates which have "width" as
// one of their durations.
// Example:
// 107 | 1 | mygk1 | 86400 | 0 | 0 | 200
// 107 | 1 | mygk1 | 86400 | 0 | 0 | 204
// ........
// 107 | 1 | mygk1 | 86400 | 1 | 0 | 201
// 107 | 1 | mygk1 | 86400 | 1 | 1 | 201
func (c *Closet) encodeSecondLevelKey(keybuf []byte, groupkey string, width uint32, secondLevelIdx int) (int, error) {
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
	// Keys with the same (secondLevelIdx mod numSecondLevel) are accessed together
	// when doing a read.
	// The in-between second-level indices are only used for reading the summary field.
	// To get better locality, the summary fields are all moved together to a
	// separate part of the key space.
	// TODO: add a check to ensure that number of buckets is perfectly divisible
	// by the second level size.
	numSecondLevel := (c.bucketizer.NumBucketsHint() / c.levelSize)
	modLevel := secondLevelIdx % numSecondLevel
	n, err = binary.PutUvarint(keybuf[curr:], uint64(modLevel))
	if err != nil {
		return 0, fmt.Errorf("error encoding mod level (%d): %w", modLevel, err)
	}
	curr += n
	n, err = binary.PutUvarint(keybuf[curr:], uint64(c.aggId))
	if err != nil {
		return 0, fmt.Errorf("error encoding agg Id (%d): %w", c.aggId, err)
	}
	curr += n
	n, err = binary.PutUvarint(keybuf[curr:], uint64(secondLevelIdx))
	if err != nil {
		return 0, fmt.Errorf("error encoding index (%d): %w", secondLevelIdx, err)
	}
	curr += n
	return curr, nil
}

// Given the groupkey and the buckets that have new values, get the keygroups
// that need to be updated.
func (c *Closet) getKeyGroupsToUpdate(groupkey string, buckets []temporal.TimeBucket) ([]hangar.KeyGroup, error) {
	// Allocate space for storing keys.
	keyLen := c.getKeyLenHint(groupkey)
	fieldLen := 10
	// Make enough space for 2 key/field writes per bucket.
	// Note: We don't allocate this byte slice using arena since the keys and
	// fields in the returned KeyGroups point to locations in this byte slice.
	buf := make([]byte, (keyLen+fieldLen)*2*len(buckets))
	// Encoded keys are stored in kgs, but the key prefix points to locations in
	// the keybuf slice.
	kgs := make([]hangar.KeyGroup, 0, len(buckets)*2)
	for _, b := range buckets {
		curr := 0

		// Encode key.
		secondLevelIdx := int(b.Index) / c.levelSize
		n, err := c.encodeSecondLevelKey(buf[curr:], groupkey, b.Width, secondLevelIdx)
		if err != nil {
			return nil, fmt.Errorf("error encoding key: %w", err)
		}
		key := buf[curr : curr+n : curr+n]
		curr += n
		// Encode bucket-specific fields.
		firstLevelIdx := int(b.Index) % c.levelSize
		n, err = binary.PutUvarint(buf[curr:], uint64(firstLevelIdx))
		if err != nil {
			return nil, fmt.Errorf("error encoding first-level index (%d): %w", firstLevelIdx, err)
		}
		field := buf[curr : curr+n : curr+n]
		curr += n
		kgs = append(kgs, hangar.KeyGroup{
			Prefix: hangar.Key{
				Data: key,
			},
			Fields: mo.Some(hangar.Fields{field}),
		})

		// Encode key for summary data.
		n, err = c.encodeThirdLevelKey(buf[curr:], groupkey, b.Width, secondLevelIdx/c.levelSize)
		if err != nil {
			return nil, fmt.Errorf("error encoding key: %w", err)
		}
		summaryKey := buf[curr : curr+n : curr+n]
		curr += n
		// Encode field for pre-aggregated summary values.
		n, err = binary.PutUvarint(buf[curr:], uint64(secondLevelIdx%c.levelSize))
		if err != nil {
			return nil, fmt.Errorf("error encoding summary field: %w", err)
		}
		preAggField := buf[curr : curr+n : curr+n]
		curr += n
		kgs = append(kgs, hangar.KeyGroup{
			Prefix: hangar.Key{
				Data: summaryKey,
			},
			Fields: mo.Some(hangar.Fields{preAggField}),
		})

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
	keyLen := c.getKeyLenHint(groupkey)
	fieldLen := 10
	// This is the maximum number of keys that we will need to read from hangar.
	numKeys := int(r.EndIdx-r.StartIdx)/c.levelSize + 2
	// The number of fields should be less than numKeys + c.firstLevelSize
	// since all but c.firstLevelSize buckets should be computed via
	// pre-aggregated fields.
	numFields := c.levelSize + numKeys
	// Note: We don't allocate this byte slice using arena since the keys and
	// fields in the returned KeyGroups point to locations in this byte slice.
	buf := make([]byte, (keyLen*numKeys)+(numFields*fieldLen))
	next := int(r.StartIdx)
	last := int(r.EndIdx)
	kgs := make([]hangar.KeyGroup, 0, numKeys)
	for next <= last {
		secondLevelIdx := next / c.levelSize
		curr := 0
		// Get the field(s) to read for this key.
		// If all the fields are in range, just use the field containing
		// pre-aggregated values. Otherwise, add all the required fields
		// individually to the keygroup.
		if (next%c.levelSize) == 0 && next+c.levelSize <= last {
			// Include the pre-computed field for this second-level index.
			// Encode key for summary data.
			n, err := c.encodeThirdLevelKey(buf[curr:], groupkey, r.Width, secondLevelIdx/c.levelSize)
			if err != nil {
				return nil, fmt.Errorf("error encoding summary key: %w", err)
			}
			summaryKey := buf[curr : curr+n : curr+n]
			curr += n
			// Encode field for pre-aggregated summary values.
			n, err = binary.PutUvarint(buf[curr:], uint64(secondLevelIdx%c.levelSize))
			if err != nil {
				return nil, fmt.Errorf("error encoding summary field: %w", err)
			}
			preAggField := buf[curr : curr+n : curr+n]
			curr += n
			kgs = append(kgs, hangar.KeyGroup{
				Prefix: hangar.Key{
					Data: summaryKey,
				},
				Fields: mo.Some(hangar.Fields{preAggField}),
			})
			next += c.levelSize
		} else {
			end := (secondLevelIdx+1)*c.levelSize - 1
			if end > last {
				end = last
			}
			// Encode key.
			n, err := c.encodeSecondLevelKey(buf[curr:], groupkey, r.Width, secondLevelIdx)
			if err != nil {
				return nil, fmt.Errorf("error encoding key: %w", err)
			}
			key := hangar.Key{Data: buf[curr : curr+n : curr+n]}
			curr += n
			fields := make(hangar.Fields, 0, end-next+1)
			for i := next; i <= end; i++ {
				// encode i-th bucket.
				firstLevelIdx := i % c.levelSize
				n, err = binary.PutUvarint(buf[curr:], uint64(firstLevelIdx))
				if err != nil {
					return nil, fmt.Errorf("error encoding first-level index (%d): %w", firstLevelIdx, err)
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

func (c *Closet) Get(ctx context.Context, keys []string, kwargs []value.Dict, store hangar.Hangar, ret []value.Value) error {
	ctx, t := timer.Start(ctx, c.tierId, "nitrous.closet.get")
	defer t.Stop()
	kgs := make([]hangar.KeyGroup, 0, len(keys)*(c.bucketizer.NumBucketsHint()/c.levelSize+2))
	// Slice containing number of buckets for each key. This is useful in
	// dividing up the read result from hangar.
	numKeys := arena.Ints.Alloc(len(keys), len(keys))
	defer arena.Ints.Free(numKeys)
	var maxFields int
	for i, key := range keys {
		duration, err := getRequestDuration(c.mr.Options(), kwargs[i])
		if err != nil {
			return fmt.Errorf("error extracting duration from request: %w", err)
		}
		buckets, err := c.bucketizer.Bucketize(c.mr, duration)
		if err != nil {
			return fmt.Errorf("error bucketizing: %w", err)
		}
		encoded, err := c.getKeyGroupsToRead(key, buckets)
		if err != nil {
			return fmt.Errorf("error encoding: %w", err)
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
		return fmt.Errorf("error getting values: %w", err)
	}
	// Slice allocated for storing the result of each field.
	vals := arena.Values.Alloc(maxFields, maxFields)
	defer arena.Values.Free(vals)
	for i := 0; i < len(keys); i++ {
		n := numKeys[i]
		intermediate := vals[:0]
		for _, vg := range vgs[:n] {
			for _, v := range vg.Values {
				if len(v) > 0 {
					uv, err := value.Unmarshal(v)
					if err != nil {
						return fmt.Errorf("error decoding value(%s): %w", uv, err)
					}
					intermediate = append(intermediate, uv)
				}
			}
		}
		ret[i], err = c.mr.Reduce(intermediate)
		if err != nil {
			return fmt.Errorf("error reducing: %w", err)
		}
		vgs = vgs[n:]
	}
	return nil
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
		keyGroupsPerBucket := len(kgs) / len(buckets)
		// Now that we have the keygroups we need to update for this groupkey,
		// we try to merge the updates with updates to the same keygroup(s) from
		// previous groupkeys.
		for j, kg := range kgs {
			ttl := ttls[j/keyGroupsPerBucket]
			p := asString(kg.Prefix.Data)
			if ptr, ok := prefixes[p]; !ok {
				prefixes[p] = len(updates)
				updates = append(updates, fieldUpdates{
					fields: kg.Fields.OrEmpty(),
					vals:   lo.Repeat(len(kg.Fields.OrEmpty()), v),
				})
				expiry = append(expiry, ttl)
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
				if ttl > expiry[ptr] {
					expiry[ptr] = ttl
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
			if len(vg.Values[j]) > 0 {
				currv, err := value.Unmarshal(vg.Values[j])
				if err != nil {
					return nil, nil, fmt.Errorf("error decoding value: %w", err)
				}
				curr[asString(f)] = currv
			}
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
