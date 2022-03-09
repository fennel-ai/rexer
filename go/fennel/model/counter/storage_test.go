package counter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
)

func TestHistogram_GetMulti_Update(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	all := []ftypes.Window{ftypes.Window_MINUTE, ftypes.Window_DAY, ftypes.Window_HOUR}

	name := ftypes.AggName("mycounter")
	histogram := rollingSum{Duration: 3600 * 14 * 30}
	key := "hello"
	count := value.Int(5)
	ts := ftypes.Timestamp(3600*24*11 + 123)
	// initially all gets will give zero
	buckets := BucketizeMoment(key, ts, count, all)
	counts, err := GetMulti(ctx, tier, name, buckets, histogram)
	assert.NoError(t, err)
	assert.Equal(t, []value.Value{value.Int(0), value.Int(0), value.Int(0)}, counts)

	// then do a couple of increments
	err = Update(ctx, tier, name, buckets, rollingSum{})
	assert.NoError(t, err)
	counts, err = GetMulti(ctx, tier, name, buckets, histogram)
	assert.NoError(t, err)
	assert.Equal(t, []value.Value{count, count, count}, counts)

	// and again?
	err = Update(ctx, tier, name, buckets, rollingSum{})
	assert.NoError(t, err)
	counts, err = GetMulti(ctx, tier, name, buckets, histogram)
	assert.NoError(t, err)
	assert.Equal(t, []value.Value{2 * count, 2 * count, 2 * count}, counts)

	// some random get still gives 0
	for i := range buckets {
		buckets[i].Index += 1
	}
	counts, err = GetMulti(ctx, tier, name, buckets, histogram)
	assert.NoError(t, err)
	assert.Equal(t, []value.Value{value.Int(0), value.Int(0), value.Int(0)}, counts)

	// finally, this composes well with bucketize Duration
	// adding 60 seconds so all minute/hour etc windows are captured
	buckets = BucketizeDuration(key, 0, ts+65, all, histogram.Zero())
	counts, err = GetMulti(ctx, tier, name, buckets, histogram)
	assert.NoError(t, err)
	var total value.Value = value.Int(0)
	for _, c := range counts {
		total, err = total.Op("+", c)
		assert.NoError(t, err)
	}
	assert.Equal(t, 2*count, total)
}
