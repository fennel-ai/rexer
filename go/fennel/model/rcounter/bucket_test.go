package rcounter

import (
	"fennel/lib/ftypes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucketizeMoment(t *testing.T) {
	key := "hello"
	count := int64(3)
	found := BucketizeMoment(key, 3601, count)
	assert.Len(t, found, 3)
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_MINUTE,
		Index:  60,
		Count:  count,
	})
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  1,
		Count:  count,
	})
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  0,
		Count:  count,
	})
}

func TestBucketizeTimeseries(t *testing.T) {
	key := "hello"
	start := ftypes.Timestamp(3601)
	end := ftypes.Timestamp(2*24*3600 + 3665) // i.e. 2 days + 1 minute + few seconds later
	// only minute/hour/day are allowed
	found, err := BucketizeTimeseries(key, start, end, ftypes.Window_NULL_WINDOW)
	assert.Error(t, err)

	found, err = BucketizeTimeseries(key, start, end, ftypes.Window_MINUTE)
	assert.Error(t, err)

	found, err = BucketizeTimeseries(key, start, end, ftypes.Window_HOUR)
	assert.NoError(t, err)
	assert.Len(t, found, 47)
	for i := 0; i < 47; i++ {
		assert.Contains(t, found, Bucket{
			Key:    key,
			Window: ftypes.Window_HOUR,
			Index:  uint64(2 + i),
			Count:  0,
		}, i)
	}
	found, err = BucketizeTimeseries(key, start, end, ftypes.Window_DAY)
	assert.NoError(t, err)
	assert.Len(t, found, 1)
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  1,
		Count:  0,
	})
}

func TestBucketizeDuration(t *testing.T) {
	key := "hello"
	// something basic
	buckets := BucketizeDuration(key, 0, 24*3600+3601)
	assert.Equal(t, 2, len(buckets))
	assert.Equal(t, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  0,
		Count:  0,
	}, buckets[0])
	assert.Equal(t, Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  24,
		Count:  0,
	}, buckets[1])

	// now let's try a more involved case
	start := ftypes.Timestamp(3601)
	end := ftypes.Timestamp(2*24*3600 + 3665) // i.e. 2 days + 1 minute + few seconds later
	buckets = BucketizeDuration(key, start, end)
	// we expect 1 day, 23 hours, 59 minutes?
	for i := 0; i < 59; i++ {
		assert.Equal(t, Bucket{
			Key:    key,
			Window: ftypes.Window_MINUTE,
			Index:  uint64(61 + i),
			Count:  0,
		}, buckets[i])
	}
	for i := 0; i < 22; i++ {
		assert.Equal(t, Bucket{
			Key:    key,
			Window: ftypes.Window_HOUR,
			Index:  uint64(2 + i),
			Count:  0,
		}, buckets[59+i])
	}
	assert.Equal(t, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  1,
		Count:  0,
	}, buckets[59+22])

	assert.Equal(t, Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  48,
		Count:  0,
	}, buckets[59+23])
	assert.Equal(t, Bucket{
		Key:    key,
		Window: ftypes.Window_MINUTE,
		Index:  60*24*2 + 60,
		Count:  0,
	}, buckets[59+24])
}

func TestMergeBuckets(t *testing.T) {
	key1 := "hello"
	key2 := "hi"
	idx1 := uint64(1)
	idx2 := uint64(2)
	window1 := ftypes.Window_HOUR
	window2 := ftypes.Window_DAY
	b1 := Bucket{Key: key1, Window: window1, Index: idx1, Count: 1}
	b1b := Bucket{Key: key1, Window: window1, Index: idx1, Count: 3}
	b2 := Bucket{Key: key2, Window: window2, Index: idx1, Count: 1}
	b3 := Bucket{Key: key1, Window: window2, Index: idx1, Count: 1}
	b4 := Bucket{Key: key1, Window: window2, Index: idx2, Count: 1}
	b4b := Bucket{Key: key1, Window: window2, Index: idx2, Count: 2}
	buckets := MergeBuckets([]Bucket{b1, b1b, b2, b3, b4, b4b})
	assert.Len(t, buckets, 4)
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window1, Index: idx1, Count: 4})
	assert.Contains(t, buckets, Bucket{Key: key2, Window: window2, Index: idx1, Count: 1})
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window2, Index: idx1, Count: 1})
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window2, Index: idx2, Count: 3})
}
