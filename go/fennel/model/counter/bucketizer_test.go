package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestBucketizeMoment(t *testing.T) {
	key := "hello"
	count := value.Int(3)
	all := []ftypes.Window{ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY}
	f := fixedWidthBucketizer{map[ftypes.Window]uint64{
		ftypes.Window_MINUTE: 1,
		ftypes.Window_HOUR:   1,
		ftypes.Window_DAY:    1,
	}, false}
	found := f.BucketizeMoment(key, 3601, count)
	assert.Len(t, found, 3)
	assert.Contains(t, found, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_MINUTE,
		Index:  60,
		Width:  1,
		Value:  count,
	})
	assert.Contains(t, found, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  1,
		Width:  1,
		Value:  count,
	})
	assert.Contains(t, found, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  0,
		Width:  1,
		Value:  count,
	})

	// also test one window at a time
	for _, w := range all {
		f := fixedWidthBucketizer{map[ftypes.Window]uint64{
			w: 1,
		}, false}
		found = f.BucketizeMoment(key, 3601, count)
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
		expected   []counter.Bucket
	}{
		{
			fixedWidthBucketizer{map[ftypes.Window]uint64{
				ftypes.Window_MINUTE: 5,
			}, false},
			key,
			3601,
			value.Int(1),
			[]counter.Bucket{{key, ftypes.Window_MINUTE, 5, 12, value.Int(1)}},
		},
		{
			fixedWidthBucketizer{map[ftypes.Window]uint64{
				ftypes.Window_MINUTE: 0, ftypes.Window_HOUR: 5, ftypes.Window_DAY: 2,
			}, false},
			key,
			47*3600 + 1,
			value.Int(1),
			[]counter.Bucket{
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
		expected   []counter.Bucket
	}{
		{
			fixedWidthBucketizer{map[ftypes.Window]uint64{
				ftypes.Window_MINUTE: 5, ftypes.Window_HOUR: 0, ftypes.Window_DAY: 1,
			}, false},
			key,
			3601,
			4459,
			value.Int(1),
			[]counter.Bucket{{key, ftypes.Window_MINUTE, 5, 13, value.Int(1)}},
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
	f := fixedWidthBucketizer{map[ftypes.Window]uint64{
		ftypes.Window_HOUR: 1,
	}, false}
	found := f.BucketizeDuration(key, start, end, v)

	assert.Len(t, found, 47)
	for i := 0; i < 47; i++ {
		assert.Contains(t, found, counter.Bucket{
			Key:    key,
			Window: ftypes.Window_HOUR,
			Index:  uint64(2 + i),
			Width:  1,
			Value:  v,
		}, i)
	}
	f = fixedWidthBucketizer{map[ftypes.Window]uint64{
		ftypes.Window_DAY: 1,
	}, false}
	found = f.BucketizeDuration(key, start, end, v)
	//found = BucketizeDuration(key, start, end, []ftypes.Window{ftypes.Window_DAY}, v)
	assert.Len(t, found, 1)
	assert.Contains(t, found, counter.Bucket{
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
	f := fixedWidthBucketizer{map[ftypes.Window]uint64{
		ftypes.Window_MINUTE: 1,
		ftypes.Window_HOUR:   1,
		ftypes.Window_DAY:    1,
	}, false}
	buckets := f.BucketizeDuration(key, 0, 24*3600+3601, v)
	assert.Equal(t, 2, len(buckets))
	assert.Equal(t, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  0,
		Width:  1,
		Value:  v,
	}, buckets[0])
	assert.Equal(t, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  24,
		Width:  1,
		Value:  v,
	}, buckets[1])

	// now let's try a more involved case
	start := ftypes.Timestamp(3601)
	end := ftypes.Timestamp(2*24*3600 + 3665) // i.e. 2 days + 1 minute + few seconds later
	buckets = f.BucketizeDuration(key, start, end, v)
	// we expect 1 day, 23 hours, 59 minutes?
	expected := make([]counter.Bucket, 0)
	for i := 0; i < 59; i++ {
		expected = append(expected, counter.Bucket{
			Key:    key,
			Window: ftypes.Window_MINUTE,
			Index:  uint64(61 + i),
			Width:  1,
			Value:  v,
		})
	}
	for i := 0; i < 22; i++ {
		expected = append(expected, counter.Bucket{
			Key:    key,
			Window: ftypes.Window_HOUR,
			Width:  1,
			Index:  uint64(2 + i),
			Value:  v,
		})
	}
	expected = append(expected, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Width:  1,
		Index:  1,
		Value:  v,
	})

	expected = append(expected, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Width:  1,
		Index:  48,
		Value:  v,
	})
	expected = append(expected, counter.Bucket{
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
	b1 := counter.Bucket{Key: key1, Window: window1, Index: idx1, Value: one}
	b1b := counter.Bucket{Key: key1, Window: window1, Index: idx1, Value: three}
	b2 := counter.Bucket{Key: key2, Window: window2, Index: idx1, Value: one}
	b3 := counter.Bucket{Key: key1, Window: window2, Index: idx1, Value: one}
	b4 := counter.Bucket{Key: key1, Window: window2, Index: idx2, Value: one}
	b4b := counter.Bucket{Key: key1, Window: window2, Index: idx2, Value: value.Int(2)}
	buckets, err := MergeBuckets(rollingSum{}, []counter.Bucket{b1, b1b, b2, b3, b4, b4b})
	assert.NoError(t, err)
	assert.Len(t, buckets, 4)
	assert.Contains(t, buckets, counter.Bucket{Key: key1, Window: window1, Index: idx1, Value: value.Int(4)})
	assert.Contains(t, buckets, counter.Bucket{Key: key2, Window: window2, Index: idx1, Value: one})
	assert.Contains(t, buckets, counter.Bucket{Key: key1, Window: window2, Index: idx1, Value: one})
	assert.Contains(t, buckets, counter.Bucket{Key: key1, Window: window2, Index: idx2, Value: three})
}

func TestBucketizeHistogram_Invalid(t *testing.T) {
	t.Parallel()
	h := NewSum([]uint64{100})
	cases := [][]*value.Dict{
		{value.NewDict(nil)},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Int(2)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Bool(true), "value": value.Int(4)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Double(1.0), "value": value.Int(3)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "value": value.Int(3)})},
		{value.NewDict(map[string]value.Value{"timestamp": value.Int(1), "value": value.Int(3)})},
	}
	for _, test := range cases {
		table := value.NewList()
		for _, d := range test {
			table.Append(d)
		}
		_, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}

func TestBucketizeHistogram_Valid(t *testing.T) {
	t.Parallel()
	h := NewSum([]uint64{100})
	actions := value.NewList()
	expected := make([]counter.Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.Int(1)
		e := value.Int(i)
		d := value.NewDict(map[string]value.Value{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     e,
		})
		actions.Append(d)
		expected = append(expected, counter.Bucket{Value: e, Window: ftypes.Window_DAY, Index: 1, Width: 1, Key: v.String()})
		expected = append(expected, counter.Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Width: 6, Index: uint64(24*10 + i*10), Value: e})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestTrailingPartial(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		key   string
		start ftypes.Timestamp
		end   ftypes.Timestamp
		w     ftypes.Window
		width uint64
		idx   uint64
		ok    bool
	}{
		{"k", 24 * 3600, 2 * 24 * 3600, ftypes.Window_HOUR, 7, 6, true},
		// buckets line up perfectly to fill [start, end], so no partial bucket left
		{"k", 24 * 3600, 2 * 24 * 3600, ftypes.Window_DAY, 1, 0, false},
		// bucket is so large that we literally get a single bucket that takes most of the duration and
		// non trailing buckets should get nothing
		{"k", 24 * 3600, 2 * 24 * 3600, ftypes.Window_HOUR, 25, 1, true},
		// bucket is so large that no "tick" of this bucket falls in the entire [start, end]
		{"k", 24 * 3600, 2 * 24 * 3600, ftypes.Window_HOUR, 50, 0, false},
	}
	for _, scene := range scenarios {
		found, ok := trailingPartial(scene.key, scene.start, scene.end, scene.w, scene.width, value.Int(4))
		assert.Equal(t, scene.ok, ok)
		if scene.ok {
			assert.Equal(t, counter.Bucket{
				Key:    scene.key,
				Window: scene.w,
				Width:  scene.width,
				Index:  scene.idx,
				Value:  value.Int(4),
			}, found)
		}
	}
}

func TestFixedSplitBucketizer_BucketizeDuration(t *testing.T) {
	f, _ := newFixedSplitBucketizer([]uint64{3, 2, 1}, []uint64{3600, 7200, 0})

	found1 := f.BucketizeDuration("key", 0, 3600, value.Int(0))
	var expected1 []counter.Bucket
	for i := 0; i < 3; i++ {
		expected1 = append(expected1, counter.Bucket{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  1200,
			Index:  uint64(i),
			Value:  value.Int(0),
		})
	}
	assert.Equal(t, expected1, found1)

	found2 := f.BucketizeDuration("key", 0, 7200, value.Int(5))
	var expected2 []counter.Bucket
	for i := 0; i < 2; i++ {
		expected2 = append(expected2, counter.Bucket{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  3600,
			Index:  uint64(i),
			Value:  value.Int(5),
		})
	}
	assert.Equal(t, expected2, found2)

	found3 := f.BucketizeDuration("key", 10800, 10800, value.Int(-5))
	expected3 := []counter.Bucket{
		{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  0,
			Index:  0,
			Value:  value.Int(-5),
		},
	}
	assert.Equal(t, expected3, found3)
}

func TestFixedSplitBucketizer_BucketizeMoment(t *testing.T) {
	f, _ := newFixedSplitBucketizer([]uint64{3, 2, 1}, []uint64{3600, 7200, 0})

	found := f.BucketizeMoment("key", 9000, value.Double(0.0))
	expected := []counter.Bucket{
		{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  1200,
			Index:  7,
			Value:  value.Double(0),
		},
		{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  3600,
			Index:  2,
			Value:  value.Double(0),
		},
		{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  0,
			Index:  0,
			Value:  value.Double(0),
		},
	}
	assert.Equal(t, expected, found)
}

func TestThirdBucketizer_BucketizeDuration(t *testing.T) {
	scenarios := []struct {
		size               uint64
		start              ftypes.Timestamp
		finish             ftypes.Timestamp
		expectedStartIndex int
		expectedNumBuckets int
	}{
		{60, 0, 120, 0, 2},
		{60, 30, 120, 0, 2},
		{60, 30, 90, 0, 2},
		{60, 60, 90, 1, 1},
		{60, 60, 120, 1, 1},
		{60, 60, 121, 1, 2},
		{0, 0, 0, 0, 1},
		{0, 9143, 1942131, 0, 1},
	}

	for _, scenario := range scenarios {
		bzer := thirdBucketizer{size: scenario.size}
		found := bzer.BucketizeDuration("key", scenario.start, scenario.finish, value.Int(0))
		assert.Len(t, found, scenario.expectedNumBuckets)
		for i := range found {
			expected := counter.Bucket{
				Key:    "key",
				Window: ftypes.Window_FOREVER,
				Width:  scenario.size,
				Index:  uint64(i + scenario.expectedStartIndex),
				Value:  value.Int(0),
			}
			assert.Equal(t, expected, found[i])
		}
	}
}

func TestThirdBucketizer_BucketizeMoment(t *testing.T) {
	scenarios := []struct {
		size          uint64
		ts            ftypes.Timestamp
		expectedIndex uint64
	}{
		{60, 0, 0},
		{60, 30, 0},
		{60, 59, 0},
		{60, 60, 1},
		{60, 120, 2},
		{0, 0, 0},
		{0, 38140, 0},
	}

	for _, scenario := range scenarios {
		bzer := thirdBucketizer{size: scenario.size}
		found := bzer.BucketizeMoment("key", scenario.ts, value.Int(0))
		assert.Len(t, found, 1)
		assert.Equal(t, counter.Bucket{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  scenario.size,
			Index:  scenario.expectedIndex,
			Value:  value.Int(0),
		}, found[0])
	}
}
