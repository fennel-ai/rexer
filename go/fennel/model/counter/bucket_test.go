package counter

import (
	"fennel/lib/ftypes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucketizeMoment(t *testing.T) {
	key := "hello"
	count := int64(3)
	all := []ftypes.Window{ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY}
	found := BucketizeMoment(key, 3601, count, all)
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

	// also test one window at a time
	for _, w := range all {
		found = BucketizeMoment(key, 3601, count, []ftypes.Window{w})
		assert.Len(t, found, 1)
		assert.Equal(t, w, found[0].Window)
	}
}

func TestBucketizeDuration_SingleWindow2(t *testing.T) {
	key := "hello"
	start := ftypes.Timestamp(3601)
	end := ftypes.Timestamp(2*24*3600 + 3665) // i.e. 2 days + 1 minute + few seconds later
	found := BucketizeDuration(key, start, end, []ftypes.Window{ftypes.Window_HOUR})
	assert.Len(t, found, 47)
	for i := 0; i < 47; i++ {
		assert.Contains(t, found, Bucket{
			Key:    key,
			Window: ftypes.Window_HOUR,
			Index:  uint64(2 + i),
			Count:  0,
		}, i)
	}
	found = BucketizeDuration(key, start, end, []ftypes.Window{ftypes.Window_DAY})
	assert.Len(t, found, 1)
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  1,
		Count:  0,
	})
}

func TestBucketizeDuration_All(t *testing.T) {
	key := "hello"
	// something basic
	all := []ftypes.Window{ftypes.Window_MINUTE, ftypes.Window_DAY, ftypes.Window_HOUR}
	buckets := BucketizeDuration(key, 0, 24*3600+3601, all)
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
	buckets = BucketizeDuration(key, start, end, all)
	// we expect 1 day, 23 hours, 59 minutes?
	expected := make([]Bucket, 0)
	for i := 0; i < 59; i++ {
		expected = append(expected, Bucket{
			Key:    key,
			Window: ftypes.Window_MINUTE,
			Index:  uint64(61 + i),
			Count:  0,
		})
	}
	for i := 0; i < 22; i++ {
		expected = append(expected, Bucket{
			Key:    key,
			Window: ftypes.Window_HOUR,
			Index:  uint64(2 + i),
			Count:  0,
		})
	}
	expected = append(expected, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  1,
		Count:  0,
	})

	expected = append(expected, Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  48,
		Count:  0,
	})
	expected = append(expected, Bucket{
		Key:    key,
		Window: ftypes.Window_MINUTE,
		Index:  60*24*2 + 60,
		Count:  0,
	})
	assert.ElementsMatch(t, expected, buckets)
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
	buckets := MergeBuckets(RollingCounter{}, []Bucket{b1, b1b, b2, b3, b4, b4b})
	assert.Len(t, buckets, 4)
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window1, Index: idx1, Count: 4})
	assert.Contains(t, buckets, Bucket{Key: key2, Window: window2, Index: idx1, Count: 1})
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window2, Index: idx1, Count: 1})
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window2, Index: idx2, Count: 3})
}
