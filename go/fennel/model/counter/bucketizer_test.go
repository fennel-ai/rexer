package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestBucketizeMoment(t *testing.T) {
	key := "hello"
	count := value.Int(3)
	all := []ftypes.Window{ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY}
	found := BucketizeMoment(key, 3601, count, all)
	assert.Len(t, found, 3)
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_MINUTE,
		Index:  60,
		Width:  1,
		Value:  count,
	})
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  1,
		Width:  1,
		Value:  count,
	})
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  0,
		Width:  1,
		Value:  count,
	})

	// also test one window at a time
	for _, w := range all {
		found = BucketizeMoment(key, 3601, count, []ftypes.Window{w})
		assert.Len(t, found, 1)
		assert.Equal(t, w, found[0].Window)
	}
}

func TestFixedWidthBucketizer_BucketizeMoment_Widths(t *testing.T) {
	key := "hi"
	scenarios := []struct {
		bucketizer Bucketizer
		key        string
		ts         ftypes.Timestamp
		v          value.Value
		expected   []Bucket
	}{
		{
			fixedWidthBucketizer{map[ftypes.Window]uint64{
				ftypes.Window_MINUTE: 5,
			}},
			key,
			3601,
			value.Int(1),
			[]Bucket{{key, ftypes.Window_MINUTE, 5, 12, value.Int(1)}},
		},
		{
			fixedWidthBucketizer{map[ftypes.Window]uint64{
				ftypes.Window_MINUTE: 0, ftypes.Window_HOUR: 5, ftypes.Window_DAY: 2,
			}},
			key,
			47*3600 + 1,
			value.Int(1),
			[]Bucket{
				{key, ftypes.Window_HOUR, 5, 9, value.Int(1)},
				{key, ftypes.Window_DAY, 2, 0, value.Int(1)},
			},
		},
	}

	for _, scenario := range scenarios {
		found := scenario.bucketizer.BucketizeMoment(scenario.key, scenario.ts, scenario.v)
		assert.ElementsMatch(t, scenario.expected, found)
	}
}

func TestFixedWidthBucketizer_BucketizeDuration(t *testing.T) {
	key := "hi"
	scenarios := []struct {
		bucketizer Bucketizer
		key        string
		start      ftypes.Timestamp
		end        ftypes.Timestamp
		v          value.Value
		expected   []Bucket
	}{
		{
			fixedWidthBucketizer{map[ftypes.Window]uint64{
				ftypes.Window_MINUTE: 5, ftypes.Window_HOUR: 0, ftypes.Window_DAY: 1,
			}},
			key,
			3601,
			4459,
			value.Int(1),
			[]Bucket{{key, ftypes.Window_MINUTE, 5, 13, value.Int(1)}},
		},
	}

	for _, scenario := range scenarios {
		found := scenario.bucketizer.BucketizeDuration(scenario.key, scenario.start, scenario.end, scenario.v)
		assert.ElementsMatch(t, scenario.expected, found)
	}

}

func TestBucketizeDuration_SingleWindow2(t *testing.T) {
	key := "hello"
	v := value.Int(91)
	start := ftypes.Timestamp(3601)
	end := ftypes.Timestamp(2*24*3600 + 3665) // i.e. 2 days + 1 minute + few seconds later
	found := BucketizeDuration(key, start, end, []ftypes.Window{ftypes.Window_HOUR}, v)

	assert.Len(t, found, 47)
	for i := 0; i < 47; i++ {
		assert.Contains(t, found, Bucket{
			Key:    key,
			Window: ftypes.Window_HOUR,
			Index:  uint64(2 + i),
			Width:  1,
			Value:  v,
		}, i)
	}
	found = BucketizeDuration(key, start, end, []ftypes.Window{ftypes.Window_DAY}, v)
	assert.Len(t, found, 1)
	assert.Contains(t, found, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  1,
		Width:  1,
		Value:  v,
	})
}

func TestBucketizeDuration_All(t *testing.T) {
	key := "hello"
	v := value.String("12")
	// something basic
	all := []ftypes.Window{ftypes.Window_MINUTE, ftypes.Window_DAY, ftypes.Window_HOUR}
	buckets := BucketizeDuration(key, 0, 24*3600+3601, all, v)
	assert.Equal(t, 2, len(buckets))
	assert.Equal(t, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  0,
		Width:  1,
		Value:  v,
	}, buckets[0])
	assert.Equal(t, Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  24,
		Width:  1,
		Value:  v,
	}, buckets[1])

	// now let's try a more involved case
	start := ftypes.Timestamp(3601)
	end := ftypes.Timestamp(2*24*3600 + 3665) // i.e. 2 days + 1 minute + few seconds later
	buckets = BucketizeDuration(key, start, end, all, v)
	// we expect 1 day, 23 hours, 59 minutes?
	expected := make([]Bucket, 0)
	for i := 0; i < 59; i++ {
		expected = append(expected, Bucket{
			Key:    key,
			Window: ftypes.Window_MINUTE,
			Index:  uint64(61 + i),
			Width:  1,
			Value:  v,
		})
	}
	for i := 0; i < 22; i++ {
		expected = append(expected, Bucket{
			Key:    key,
			Window: ftypes.Window_HOUR,
			Width:  1,
			Index:  uint64(2 + i),
			Value:  v,
		})
	}
	expected = append(expected, Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Width:  1,
		Index:  1,
		Value:  v,
	})

	expected = append(expected, Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Width:  1,
		Index:  48,
		Value:  v,
	})
	expected = append(expected, Bucket{
		Key:    key,
		Window: ftypes.Window_MINUTE,
		Width:  1,
		Index:  60*24*2 + 60,
		Value:  v,
	})
	assert.ElementsMatch(t, expected, buckets)
}

func TestMergeBuckets(t *testing.T) {
	key1 := "hello"
	key2 := "hi"
	idx1 := uint64(1)
	idx2 := uint64(2)
	one := value.Int(1)
	three := value.Int(3)
	window1 := ftypes.Window_HOUR
	window2 := ftypes.Window_DAY
	b1 := Bucket{Key: key1, Window: window1, Index: idx1, Value: one}
	b1b := Bucket{Key: key1, Window: window1, Index: idx1, Value: three}
	b2 := Bucket{Key: key2, Window: window2, Index: idx1, Value: one}
	b3 := Bucket{Key: key1, Window: window2, Index: idx1, Value: one}
	b4 := Bucket{Key: key1, Window: window2, Index: idx2, Value: one}
	b4b := Bucket{Key: key1, Window: window2, Index: idx2, Value: value.Int(2)}
	buckets, err := MergeBuckets(rollingSum{}, []Bucket{b1, b1b, b2, b3, b4, b4b})
	assert.NoError(t, err)
	assert.Len(t, buckets, 4)
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window1, Index: idx1, Value: value.Int(4)})
	assert.Contains(t, buckets, Bucket{Key: key2, Window: window2, Index: idx1, Value: one})
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window2, Index: idx1, Value: one})
	assert.Contains(t, buckets, Bucket{Key: key1, Window: window2, Index: idx2, Value: three})
}

func TestBucketizeHistogram_Invalid(t *testing.T) {
	t.Parallel()
	h := NewSum("somename", 100)
	cases := [][]value.Dict{
		{value.Dict{}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Bool(true), "value": value.Int(4)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Double(1.0), "value": value.Int(3)}},
		{value.Dict{"groupkey": value.Int(1), "value": value.Int(3)}},
		{value.Dict{"timestamp": value.Int(1), "value": value.Int(3)}},
	}
	for _, test := range cases {
		table := value.List{}
		for _, d := range test {
			assert.NoError(t, table.Append(d))
		}
		_, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}

func TestBucketizeHistogram_Valid(t *testing.T) {
	t.Parallel()
	h := NewSum("somename", 100)
	actions := value.List{}
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.Int(1)
		e := value.Int(i)
		d := value.Dict{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     e,
		}
		assert.NoError(t, actions.Append(d))
		expected = append(expected, Bucket{Value: e, Window: ftypes.Window_DAY, Index: 1, Width: 1, Key: v.String()})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_HOUR, Width: 1, Index: uint64(24 + i), Value: e})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Width: 1, Index: uint64(24*60 + i*60), Value: e})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}
