package main

import (
	"fennel/lib/action"
	"fennel/lib/counter"
	"fennel/lib/utils"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func verify(table CounterTable, t *testing.T, expected uint64, ct counter.CounterType, window counter.Window, key counter.Key, ts action.Timestamp) {
	count, err := table.counterGet(counter.GetCountRequest{CounterType: ct, Window: window, Key: key, Timestamp: ts})
	assert.NoError(t, err)
	assert.Equal(t, expected, count)
}

func TestCounterStorage(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewCounterTable(DB)
	assert.NoError(t, err)

	ct := counter.CounterType_USER_LIKE
	key := counter.Key{1, 2, 3}
	deltas := map[counter.Window]action.Timestamp{
		counter.Window_HOUR:    3600,
		counter.Window_DAY:     24 * 3600,
		counter.Window_WEEK:    7 * 24 * 3600,
		counter.Window_MONTH:   30 * 24 * 3600,
		counter.Window_QUARTER: 90 * 24 * 3600,
		counter.Window_YEAR:    365 * 24 * 3600,
	}
	for w, delta := range deltas {
		ts := action.Timestamp(1)
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
	ct := counter.CounterType_USER_LIKE
	key := counter.Key{1, 2, 3}
	ts := action.Timestamp(1)
	// initially we haven't done anything, so all gets should be 0
	verify(table, t, 0, ct, counter.Window_FOREVER, key, ts)

	//now let's do a single increment and verify that specific window works
	err = table.counterIncrement(ct, counter.Window_FOREVER, key, ts, 3)
	assert.NoError(t, err)
	verify(table, t, 3, ct, counter.Window_FOREVER, key, ts)

	// another increment some time later which should also show up
	next := ts + 1e6
	err = table.counterIncrement(ct, counter.Window_FOREVER, key, next, 2)
	assert.NoError(t, err)
	verify(table, t, 5, ct, counter.Window_FOREVER, key, next)

	// and no matter how far we go, we always see this value
	verify(table, t, 5, ct, counter.Window_FOREVER, key, ts+3*10e9)
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
