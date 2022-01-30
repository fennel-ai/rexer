package counter

import (
	"fennel/lib/ftypes"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetIncrement(t *testing.T) {
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)

	name := ftypes.AggName("mycounter")
	key := "hello"
	count := int64(5)
	ts := ftypes.Timestamp(3600*24*11 + 123)
	// initially all gets will give zero
	buckets := BucketizeMoment(key, ts, count)
	counts, err := GetMulti(instance, name, buckets)
	assert.NoError(t, err)
	assert.Equal(t, []int64{0, 0, 0}, counts)

	// then do a couple of increments
	err = IncrementMulti(instance, name, buckets)
	assert.NoError(t, err)
	counts, err = GetMulti(instance, name, buckets)
	assert.NoError(t, err)
	assert.Equal(t, []int64{count, count, count}, counts)

	// and again?
	err = IncrementMulti(instance, name, buckets)
	assert.NoError(t, err)
	counts, err = GetMulti(instance, name, buckets)
	assert.NoError(t, err)
	assert.Equal(t, []int64{2 * count, 2 * count, 2 * count}, counts)
	// and single get also works
	for _, b := range buckets {
		f, err := Get(instance, name, b)
		assert.NoError(t, err)
		assert.Equal(t, 2*count, f)
	}
	// some random get still gives 0
	for i, _ := range buckets {
		buckets[i].Index += 1
	}
	counts, err = GetMulti(instance, name, buckets)
	assert.NoError(t, err)
	assert.Equal(t, []int64{0, 0, 0}, counts)

	// finally, this composes well with bucketize duration
	// adding 60 seconds so all minute/hour etc windows are captured
	buckets = BucketizeDuration(key, 0, ts+65)
	counts, err = GetMulti(instance, name, buckets)
	assert.NoError(t, err)
	total := int64(0)
	for _, c := range counts {
		total += c
	}
	assert.Equal(t, 2*count, total)
}
