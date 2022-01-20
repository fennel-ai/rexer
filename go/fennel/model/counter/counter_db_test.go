package counter

import (
	"fennel/instance"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func verify(this instance.Instance, t *testing.T, expected uint64, ct counter.CounterType, window ftypes.Window, key ftypes.Key, ts ftypes.Timestamp) {
	count, err := Get(this, counter.GetCountRequest{CounterType: ct, Window: window, Key: key, Timestamp: ts})
	assert.NoError(t, err)
	assert.Equal(t, expected, count)
}

func TestCounterStorage(t *testing.T) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)

	ct := counter.CounterType_USER_LIKE
	key := ftypes.Key{1, 2, 3}
	deltas := map[ftypes.Window]ftypes.Timestamp{
		ftypes.Window_HOUR:    3600,
		ftypes.Window_DAY:     24 * 3600,
		ftypes.Window_WEEK:    7 * 24 * 3600,
		ftypes.Window_MONTH:   30 * 24 * 3600,
		ftypes.Window_QUARTER: 90 * 24 * 3600,
		ftypes.Window_YEAR:    365 * 24 * 3600,
	}
	for w, delta := range deltas {
		ts := ftypes.Timestamp(1)
		// initially we haven't done anything, so all gets should be 0
		verify(this, t, 0, ct, w, key, ts)

		//now let's do a single increment and verify that specific window works
		err = Increment(this, ct, w, key, ts, 3)
		assert.NoError(t, err)
		verify(this, t, 3, ct, w, key, ts)

		// another increment at same timestamp works
		err = Increment(this, ct, w, key, ts, 4)
		assert.NoError(t, err)
		verify(this, t, 7, ct, w, key, ts)

		// another increment some time later which should also show up
		next := ts + delta/2
		err = Increment(this, ct, w, key, next, 2)
		assert.NoError(t, err)
		verify(this, t, 9, ct, w, key, next)

		// now let's do a query full period later and verify it isn't showing older values
		verify(this, t, 2, ct, w, key, ts+delta)
	}
}

func TestForeverWindow(t *testing.T) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)

	ct := counter.CounterType_USER_LIKE
	key := ftypes.Key{1, 2, 3}
	ts := ftypes.Timestamp(1)
	// initially we haven't done anything, so all gets should be 0
	verify(this, t, 0, ct, ftypes.Window_FOREVER, key, ts)

	//now let's do a single increment and verify that specific window works
	err = Increment(this, ct, ftypes.Window_FOREVER, key, ts, 3)
	assert.NoError(t, err)
	verify(this, t, 3, ct, ftypes.Window_FOREVER, key, ts)

	// another increment some time later which should also show up
	next := ts + 1e6
	err = Increment(this, ct, ftypes.Window_FOREVER, key, next, 2)
	assert.NoError(t, err)
	verify(this, t, 5, ct, ftypes.Window_FOREVER, key, next)

	// and no matter how far we go, we always see this value
	verify(this, t, 5, ct, ftypes.Window_FOREVER, key, ts+3*10e9)
}

func TestLongKey(t *testing.T) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)
	// it should not be possible to set a key longer than 256 chars
	b := bucket{1, 2, 3, utils.RandString(257), 1}
	err = dbIncrement(this, b)
	assert.Error(t, err)

	// but it should be fine with key of 256 chars
	b = bucket{1, 2, 3, utils.RandString(256), 1}
	err = dbIncrement(this, b)
	assert.NoError(t, err)
}
