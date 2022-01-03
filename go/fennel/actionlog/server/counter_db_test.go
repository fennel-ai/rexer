package main

import (
	"fennel/actionlog/lib"
	"github.com/stretchr/testify/assert"
	"testing"
)

func verify(t *testing.T, expected uint64, ct lib.CounterType, window lib.Window, key lib.Key, ts lib.Timestamp) {
	count, err := counterGet(lib.GetCountRequest{CounterType: ct, Window: window, Key: key, Timestamp: ts})
	assert.NoError(t, err)
	assert.Equal(t, expected, count)
}

func TestCounterStorage(t *testing.T) {
	t.Cleanup(dbInit)
	ct := lib.USER_LIKE
	key := lib.Key{1, 2, 3}
	deltas := map[lib.Window]lib.Timestamp{
		lib.HOUR:    3600,
		lib.DAY:     24 * 3600,
		lib.WEEK:    7 * 24 * 3600,
		lib.MONTH:   30 * 24 * 3600,
		lib.QUARTER: 90 * 24 * 3600,
		lib.YEAR:    365 * 24 * 3600,
	}
	for w, delta := range deltas {
		ts := lib.Timestamp(1)
		// initially we haven't done anything, so all gets should be 0
		verify(t, 0, ct, w, key, ts)

		//now let's do a single increment and verify that specific window works
		err := counterIncrement(ct, w, key, ts, 3)
		assert.NoError(t, err)
		verify(t, 3, ct, w, key, ts)

		// another increment at same timestamp works
		err = counterIncrement(ct, w, key, ts, 4)
		assert.NoError(t, err)
		verify(t, 7, ct, w, key, ts)

		// another increment some time later which should also show up
		next := ts + delta/2
		err = counterIncrement(ct, w, key, next, 2)
		assert.NoError(t, err)
		verify(t, 9, ct, w, key, next)

		// now let's do a query full period later and verify it isn't showing older values
		verify(t, 2, ct, w, key, ts+delta)
	}
}

func TestForeverWindow(t *testing.T) {
	t.Cleanup(dbInit)
	ct := lib.USER_LIKE
	key := lib.Key{1, 2, 3}
	ts := lib.Timestamp(1)
	// initially we haven't done anything, so all gets should be 0
	verify(t, 0, ct, lib.FOREVER, key, ts)

	//now let's do a single increment and verify that specific window works
	err := counterIncrement(ct, lib.FOREVER, key, ts, 3)
	assert.NoError(t, err)
	verify(t, 3, ct, lib.FOREVER, key, ts)

	// another increment some time later which should also show up
	next := ts + 1e6
	err = counterIncrement(ct, lib.FOREVER, key, next, 2)
	assert.NoError(t, err)
	verify(t, 5, ct, lib.FOREVER, key, next)

	// and no matter how far we go, we always see this value
	verify(t, 5, ct, lib.FOREVER, key, ts+3*10e9)
}

func TestCheckpoint(t *testing.T) {
	t.Cleanup(dbInit)
	ct1 := lib.USER_LIKE
	zero := lib.OidType(0)
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err := counterDBGetCheckpoint(ct1)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	// now set a checkpoint
	expected1 := lib.OidType(1)
	err = counterDBSetCheckpoint(ct1, expected1)
	assert.NoError(t, err)
	// and reading it now, we get new value
	checkpoint, err = counterDBGetCheckpoint(ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected1, checkpoint)

	//can reset it again
	expected2 := lib.OidType(2)
	err = counterDBSetCheckpoint(ct1, expected2)
	assert.NoError(t, err)
	checkpoint, err = counterDBGetCheckpoint(ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)

	// meanwhile other counter types aren't affected
	var ct2 lib.CounterType = lib.USER_SHARE
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err = counterDBGetCheckpoint(ct2)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	expected3 := lib.OidType(51)
	err = counterDBSetCheckpoint(ct2, expected3)
	assert.NoError(t, err)

	checkpoint, err = counterDBGetCheckpoint(ct2)
	assert.NoError(t, err)
	assert.Equal(t, expected3, checkpoint)

	// meanwhile checkpoint for original CT isn't affected
	checkpoint, err = counterDBGetCheckpoint(ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)
}
