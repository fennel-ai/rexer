package temporal

import (
	"testing"
	"time"

	"fennel/lib/aggregate"
	"fennel/lib/counter"
	"fennel/lib/ftypes"

	"github.com/raulk/clock"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

func TestBucketizer(t *testing.T) {
	opts := aggregate.Options{
		AggType:   "sum",
		Durations: []uint32{24 * 3600},
	}
	mr, err := counter.ToMergeReduce(ftypes.AggId(1), opts)
	assert.NoError(t, err)
	clock := clock.NewMock()
	clock.Set(time.Now())
	b := NewFixedWidthBucketizer(100, clock)
	buckets, _, err := b.BucketizeMoment(mr, uint32(clock.Now().Unix()))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(buckets))
	assert.Equal(t, uint32(864), buckets[0].Width)
	assert.Equal(t, uint32(clock.Now().Unix())/864, buckets[0].Index)
	curr := buckets[0]

	bucketRange, err := b.Bucketize(mr, mo.Some[uint32](24*3600))
	assert.NoError(t, err)
	assert.Equal(t, curr.Width, bucketRange.Width)
	assert.EqualValues(t, 100, bucketRange.EndIdx-bucketRange.StartIdx)
	assert.GreaterOrEqual(t, curr.Index, bucketRange.StartIdx)
	assert.LessOrEqual(t, curr.Index, bucketRange.EndIdx)

	clock.Add(25 * time.Hour)
	bucketRange, err = b.Bucketize(mr, mo.Some[uint32](24*3600))
	assert.NoError(t, err)
	assert.Equal(t, curr.Width, bucketRange.Width)
	assert.Less(t, curr.Index, bucketRange.StartIdx)
}
