package counter

import (
	"fennel/lib/aggregate"
	"fennel/lib/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertBucket(t *testing.T) {
	b := Bucket{
		Key:    "foo",
		Width:  20,
		Window: 1000,
		Index:  12,
		Value:  value.Int(22),
	}
	pb, err := ToProtoBucket(b)
	assert.NoError(t, err)

	b2, err := FromProtoBucket(pb)
	assert.NoError(t, err)
	assert.Equal(t, b, b2)
}

func TestConvertAggregateDelta(t *testing.T) {
	b := []Bucket{
		{
			Key:    "foo",
			Width:  21,
			Window: 1000,
			Index:  12,
			Value:  value.Int(22),
		},
		{
			Key:    "foo2",
			Width:  20,
			Window: 1001,
			Index:  21,
			Value:  value.Int(21),
		},
	}
	opts := aggregate.Options{
		AggType:   "foo",
		Durations: []uint64{12},
		Window:    12,
		Limit:     200,
		Normalize: false,
	}
	a := AggregateDelta{
		AggId:   12,
		Buckets: b,
		Options: opts,
	}
	pa, err := ToProtoAggregateDelta(12, opts, b)
	assert.NoError(t, err)

	a2, err := FromProtoAggregateDelta(&pa)
	assert.NoError(t, err)
	assert.Equal(t, a, a2)
}
