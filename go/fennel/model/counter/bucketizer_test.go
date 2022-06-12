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
	all := []ftypes.Window{ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY}
	f := fixedWidthBucketizer{map[ftypes.Window]uint32{
		ftypes.Window_MINUTE: 1,
		ftypes.Window_HOUR:   1,
		ftypes.Window_DAY:    1,
	}, false}
	found := f.BucketizeMoment(key, 3601)
	assert.Len(t, found, 3)
	assert.Contains(t, found, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_MINUTE,
		Index:  60,
		Width:  1,
	})
	assert.Contains(t, found, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_HOUR,
		Index:  1,
		Width:  1,
	})
	assert.Contains(t, found, counter.Bucket{
		Key:    key,
		Window: ftypes.Window_DAY,
		Index:  0,
		Width:  1,
	})

	// also test one window at a time
	for _, w := range all {
		f := fixedWidthBucketizer{map[ftypes.Window]uint32{
			w: 1,
		}, false}
		found = f.BucketizeMoment(key, 3601)
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
			fixedWidthBucketizer{map[ftypes.Window]uint32{
				ftypes.Window_MINUTE: 5,
			}, false},
			key,
			3601,
			value.Int(1),
			[]counter.Bucket{{key, ftypes.Window_MINUTE, 5, 12}},
		},
		{
			fixedWidthBucketizer{map[ftypes.Window]uint32{
				ftypes.Window_MINUTE: 0, ftypes.Window_HOUR: 5, ftypes.Window_DAY: 2,
			}, false},
			key,
			47*3600 + 1,
			value.Int(1),
			[]counter.Bucket{
				{key, ftypes.Window_HOUR, 5, 9},
				{key, ftypes.Window_DAY, 2, 0},
			},
		},
	}

	for _, scenario := range scenarios {
		found := scenario.bucketizer.BucketizeMoment(scenario.key, scenario.ts)
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
		expected   []counter.BucketList
	}{
		{
			fixedWidthBucketizer{map[ftypes.Window]uint32{
				ftypes.Window_MINUTE: 5, ftypes.Window_HOUR: 0, ftypes.Window_DAY: 1,
			}, false},
			key,
			3601,
			4459,
			value.Int(1),
			[]counter.BucketList{{key, ftypes.Window_MINUTE, 5, 13, 13}},
		},
	}

	for _, scenario := range scenarios {
		found := scenario.bucketizer.BucketizeDuration(scenario.key, scenario.start, scenario.end)
		assert.ElementsMatch(t, scenario.expected, found)
	}

}

func TestBucketizeDuration_SingleWindow2(t *testing.T) {
	key := "hello"
	start := ftypes.Timestamp(3601)
	end := ftypes.Timestamp(2*24*3600 + 3665) // i.e. 2 days + 1 minute + few seconds later
	f := fixedWidthBucketizer{map[ftypes.Window]uint32{
		ftypes.Window_HOUR: 1,
	}, false}
	found := f.BucketizeDuration(key, start, end)

	expected := []counter.BucketList{{
		Key:        key,
		Window:     ftypes.Window_HOUR,
		Width:      1,
		StartIndex: 2,
		EndIndex:   48,
	}}
	assert.Equal(t, expected, found)
	f = fixedWidthBucketizer{map[ftypes.Window]uint32{
		ftypes.Window_DAY: 1,
	}, false}
	found = f.BucketizeDuration(key, start, end)
	expected = []counter.BucketList{{
		Key:        key,
		Window:     ftypes.Window_DAY,
		Width:      1,
		StartIndex: 1,
		EndIndex:   1,
	}}
}

func TestBucketizeDuration_All(t *testing.T) {
	key := "hello"
	// something basic
	f := fixedWidthBucketizer{map[ftypes.Window]uint32{
		ftypes.Window_MINUTE: 1,
		ftypes.Window_HOUR:   1,
		ftypes.Window_DAY:    1,
	}, false}
	found := f.BucketizeDuration(key, 0, 24*3600+3601)
	assert.Equal(t, 2, len(found))
	assert.Equal(t, counter.BucketList{
		Key:        key,
		Window:     ftypes.Window_DAY,
		Width:      1,
		StartIndex: 0,
		EndIndex:   0,
	}, found[0])
	assert.Equal(t, counter.BucketList{
		Key:        key,
		Window:     ftypes.Window_HOUR,
		Width:      1,
		StartIndex: 24,
		EndIndex:   24,
	}, found[1])

	// now let's try a more involved case
	start := ftypes.Timestamp(3601)
	end := ftypes.Timestamp(2*24*3600 + 3665) // i.e. 2 days + 1 minute + few seconds later
	found = f.BucketizeDuration(key, start, end)
	// we expect 1 day, 23 hours, 59 minutes?
	expected := make([]counter.BucketList, 0)
	expected = append(expected, counter.BucketList{
		Key:        key,
		Window:     ftypes.Window_MINUTE,
		StartIndex: 61,
		EndIndex:   119,
		Width:      1,
	})
	expected = append(expected, counter.BucketList{
		Key:        key,
		Window:     ftypes.Window_HOUR,
		Width:      1,
		StartIndex: 2,
		EndIndex:   23,
	})
	expected = append(expected, counter.BucketList{
		Key:        key,
		Window:     ftypes.Window_DAY,
		Width:      1,
		StartIndex: 1,
		EndIndex:   1,
	})

	expected = append(expected, counter.BucketList{
		Key:        key,
		Window:     ftypes.Window_HOUR,
		Width:      1,
		StartIndex: 48,
		EndIndex:   48,
	})
	expected = append(expected, counter.BucketList{
		Key:        key,
		Window:     ftypes.Window_MINUTE,
		Width:      1,
		StartIndex: 60*24*2 + 60,
		EndIndex:   60*24*2 + 60,
	})
	assert.ElementsMatch(t, expected, found)
}

func TestMergeBuckets(t *testing.T) {
	key1 := "hello"
	key2 := "hi"
	idx1 := uint32(1)
	idx2 := uint32(2)
	one := value.Int(1)
	three := value.Int(3)
	window1 := ftypes.Window_HOUR
	window2 := ftypes.Window_DAY
	b1 := counter.Bucket{Key: key1, Window: window1, Index: idx1}
	b1b := counter.Bucket{Key: key1, Window: window1, Index: idx1}
	b2 := counter.Bucket{Key: key2, Window: window2, Index: idx1}
	b3 := counter.Bucket{Key: key1, Window: window2, Index: idx1}
	b4 := counter.Bucket{Key: key1, Window: window2, Index: idx2}
	b4b := counter.Bucket{Key: key1, Window: window2, Index: idx2}
	buckets, vals, err := MergeBuckets(rollingSum{}, []counter.Bucket{b1, b1b, b2, b3, b4, b4b}, []value.Value{one, three, one, one, one, value.Int(2)})
	assert.NoError(t, err)
	assert.Len(t, buckets, 4)
	assert.ElementsMatch(t, buckets, []counter.Bucket{
		{Key: key1, Window: window1, Index: idx1},
		{Key: key2, Window: window2, Index: idx1},
		{Key: key1, Window: window2, Index: idx1},
		{Key: key1, Window: window2, Index: idx2},
	})
	assert.ElementsMatch(t, vals, []value.Value{value.Int(4), one, one, three})
}

func TestBucketizeHistogram_Invalid(t *testing.T) {
	t.Parallel()
	cases := [][]value.Dict{
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
		_, _, err := Bucketize(sixMinutelyBucketizer, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}

func TestBucketizeHistogram_Valid(t *testing.T) {
	t.Parallel()
	actions := value.NewList()
	expected := make([]counter.Bucket, 0)
	expVals := make([]value.Value, 0)
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
		expected = append(expected, counter.Bucket{Window: ftypes.Window_DAY, Index: 1, Width: 1, Key: v.String()})
		expVals = append(expVals, e)
		expected = append(expected, counter.Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Width: 6, Index: uint32(24*10 + i*10)})
		expVals = append(expVals, e)
	}
	buckets, vals, err := Bucketize(sixMinutelyBucketizer, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
	assert.ElementsMatch(t, expVals, vals)
}

func TestTrailingPartial(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		key   string
		start ftypes.Timestamp
		end   ftypes.Timestamp
		w     ftypes.Window
		width uint32
		idx   uint32
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
		found, ok := trailingPartial(scene.key, scene.start, scene.end, scene.w, scene.width)
		assert.Equal(t, scene.ok, ok)
		if scene.ok {
			assert.Equal(t, counter.BucketList{
				Key:        scene.key,
				Window:     scene.w,
				Width:      scene.width,
				StartIndex: scene.idx,
				EndIndex:   scene.idx,
			}, found)
		}
	}
}

func TestFixedSplitBucketizer_BucketizeDuration(t *testing.T) {
	f, _ := newFixedSplitBucketizer([]uint64{3, 2, 1}, []uint64{3600, 7200, 0})

	found1 := f.BucketizeDuration("key", 0, 3600)
	expected1 := []counter.BucketList{{
		Key:        "key",
		Window:     ftypes.Window_FOREVER,
		Width:      1200,
		StartIndex: 0,
		EndIndex:   2,
	}}
	assert.Equal(t, expected1, found1)

	found2 := f.BucketizeDuration("key", 0, 7200)
	expected2 := []counter.BucketList{{
		Key:        "key",
		Window:     ftypes.Window_FOREVER,
		Width:      3600,
		StartIndex: 0,
		EndIndex:   1,
	}}
	assert.Equal(t, expected2, found2)

	found3 := f.BucketizeDuration("key", 10800, 10800)
	expected3 := []counter.BucketList{
		{
			Key:        "key",
			Window:     ftypes.Window_FOREVER,
			Width:      0,
			StartIndex: 0,
			EndIndex:   0,
		},
	}
	assert.Equal(t, expected3, found3)
}

func TestFixedSplitBucketizer_BucketizeMoment(t *testing.T) {
	f, _ := newFixedSplitBucketizer([]uint64{3, 2, 1}, []uint64{3600, 7200, 0})

	found := f.BucketizeMoment("key", 9000)
	expected := []counter.Bucket{
		{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  1200,
			Index:  7,
		},
		{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  3600,
			Index:  2,
		},
		{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  0,
			Index:  0,
		},
	}
	assert.Equal(t, expected, found)
}

func TestThirdBucketizer_BucketizeDuration(t *testing.T) {
	scenarios := []struct {
		size               uint32
		start              ftypes.Timestamp
		finish             ftypes.Timestamp
		expectedStartIndex uint32
		expectedEndIndex   uint32
	}{
		{60, 0, 120, 0, 1},
		{60, 30, 120, 0, 1},
		{60, 30, 90, 0, 1},
		{60, 60, 90, 1, 1},
		{60, 60, 120, 1, 1},
		{60, 60, 121, 1, 2},
		{0, 0, 0, 0, 0},
		{0, 9143, 1942131, 0, 0},
	}

	for _, scenario := range scenarios {
		bzer := thirdBucketizer{size: scenario.size}
		found := bzer.BucketizeDuration("key", scenario.start, scenario.finish)
		assert.Len(t, found, 1)
		expected := counter.BucketList{
			Key:        "key",
			Window:     ftypes.Window_FOREVER,
			Width:      scenario.size,
			StartIndex: scenario.expectedStartIndex,
			EndIndex:   scenario.expectedEndIndex,
		}
		assert.Equal(t, expected, found[0])
	}
}

func TestThirdBucketizer_BucketizeMoment(t *testing.T) {
	scenarios := []struct {
		size          uint32
		ts            ftypes.Timestamp
		expectedIndex uint32
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
		found := bzer.BucketizeMoment("key", scenario.ts)
		assert.Len(t, found, 1)
		assert.Equal(t, counter.Bucket{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  scenario.size,
			Index:  scenario.expectedIndex,
		}, found[0])
	}
}
