package main

import (
	"fennel/data/lib"
	profileLib "fennel/profile/lib"
	"fennel/test"
	"fennel/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func verify(table CounterTable, t *testing.T, expected uint64, ct lib.CounterType, window lib.Window, key lib.Key, ts lib.Timestamp) {
	count, err := table.counterGet(lib.GetCountRequest{CounterType: ct, Window: window, Key: key, Timestamp: ts})
	assert.NoError(t, err)
	assert.Equal(t, expected, count)
}

func TestCounterStorage(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewCounterTable(DB)
	assert.NoError(t, err)

	ct := lib.CounterType_USER_LIKE
	key := lib.Key{1, 2, 3}
	deltas := map[lib.Window]lib.Timestamp{
		lib.Window_HOUR:    3600,
		lib.Window_DAY:     24 * 3600,
		lib.Window_WEEK:    7 * 24 * 3600,
		lib.Window_MONTH:   30 * 24 * 3600,
		lib.Window_QUARTER: 90 * 24 * 3600,
		lib.Window_YEAR:    365 * 24 * 3600,
	}
	for w, delta := range deltas {
		ts := lib.Timestamp(1)
		// initially we haven't done anything, so all gets should be 0
		verify(table, t, 0, ct, w, key, ts)

		//now let's do a single increment and verify that specific window works
		err = table.counterIncrement(ct, w, key, ts, 3)
		assert.NoError(t, err)
		verify(table, t, 3, ct, w, key, ts)

		// another increment at same timestamp works
		err = table.counterIncrement(ct, w, key, ts, 4)
		assert.NoError(t, err)
		verify(table, t, 7, ct, w, key, ts)

		// another increment some time later which should also show up
		next := ts + delta/2
		err = table.counterIncrement(ct, w, key, next, 2)
		assert.NoError(t, err)
		verify(table, t, 9, ct, w, key, next)

		// now let's do a query full period later and verify it isn't showing older values
		verify(table, t, 2, ct, w, key, ts+delta)
	}
}

func TestForeverWindow(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewCounterTable(DB)
	assert.NoError(t, err)
	ct := lib.CounterType_USER_LIKE
	key := lib.Key{1, 2, 3}
	ts := lib.Timestamp(1)
	// initially we haven't done anything, so all gets should be 0
	verify(table, t, 0, ct, lib.Window_FOREVER, key, ts)

	//now let's do a single increment and verify that specific window works
	err = table.counterIncrement(ct, lib.Window_FOREVER, key, ts, 3)
	assert.NoError(t, err)
	verify(table, t, 3, ct, lib.Window_FOREVER, key, ts)

	// another increment some time later which should also show up
	next := ts + 1e6
	err = table.counterIncrement(ct, lib.Window_FOREVER, key, next, 2)
	assert.NoError(t, err)
	verify(table, t, 5, ct, lib.Window_FOREVER, key, next)

	// and no matter how far we go, we always see this value
	verify(table, t, 5, ct, lib.Window_FOREVER, key, ts+3*10e9)
}

func TestLongKey(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewCounterTable(DB)
	assert.NoError(t, err)
	// it should not be possible to set a key longer than 256 chars
	bucket := CounterBucket{1, 2, 3, utils.RandString(257), 1}
	err = table.counterDBIncrement(bucket)
	assert.Error(t, err)

	// but it should be fine with key of 256 chars
	bucket = CounterBucket{1, 2, 3, utils.RandString(256), 1}
	err = table.counterDBIncrement(bucket)
	assert.NoError(t, err)
}

func TestCheckpoint(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewCheckpointTable(DB)
	assert.NoError(t, err)
	ct1 := lib.CounterType_USER_LIKE
	zero := profileLib.OidType(0)
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err := table.counterDBGetCheckpoint(ct1)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	// now set a checkpoint
	expected1 := profileLib.OidType(1)
	err = table.counterDBSetCheckpoint(ct1, expected1)
	assert.NoError(t, err)
	// and reading it now, we get new value
	checkpoint, err = table.counterDBGetCheckpoint(ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected1, checkpoint)

	//can reset it again
	expected2 := profileLib.OidType(2)
	err = table.counterDBSetCheckpoint(ct1, expected2)
	assert.NoError(t, err)
	checkpoint, err = table.counterDBGetCheckpoint(ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)

	// meanwhile other counter types aren't affected
	var ct2 lib.CounterType = lib.CounterType_USER_SHARE
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err = table.counterDBGetCheckpoint(ct2)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	expected3 := profileLib.OidType(51)
	err = table.counterDBSetCheckpoint(ct2, expected3)
	assert.NoError(t, err)

	checkpoint, err = table.counterDBGetCheckpoint(ct2)
	assert.NoError(t, err)
	assert.Equal(t, expected3, checkpoint)

	// meanwhile checkpoint for original CT isn't affected
	checkpoint, err = table.counterDBGetCheckpoint(ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)
}
