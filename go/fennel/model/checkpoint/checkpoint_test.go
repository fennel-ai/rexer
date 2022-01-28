package checkpoint

import (
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckpoint(t *testing.T) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)
	ct1 := counter.CounterType_USER_LIKE
	zero := ftypes.OidType(0)
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err := GetCheckpoint(this, ct1)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	// now set a checkpoint
	expected1 := ftypes.OidType(1)
	err = SetCheckpoint(this, ct1, expected1)
	assert.NoError(t, err)
	// and reading it now, we get new value
	checkpoint, err = GetCheckpoint(this, ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected1, checkpoint)

	//can reset it again
	expected2 := ftypes.OidType(2)
	err = SetCheckpoint(this, ct1, expected2)
	assert.NoError(t, err)
	checkpoint, err = GetCheckpoint(this, ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)

	// meanwhile other counter types aren't affected
	var ct2 counter.CounterType = counter.CounterType_USER_SHARE
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err = GetCheckpoint(this, ct2)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	expected3 := ftypes.OidType(51)
	err = SetCheckpoint(this, ct2, expected3)
	assert.NoError(t, err)

	checkpoint, err = GetCheckpoint(this, ct2)
	assert.NoError(t, err)
	assert.Equal(t, expected3, checkpoint)

	// meanwhile checkpoint for original CT isn't affected
	checkpoint, err = GetCheckpoint(this, ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)
}

func TestCheckpoint2(t *testing.T) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)
	aggtype := ftypes.AggType("rolling_counter")
	aggname := ftypes.AggName("mycounter")
	zero := ftypes.OidType(0)
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err := GetCheckpoint2(this, aggtype, aggname)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	// now set a checkpoint
	expected1 := ftypes.OidType(1)
	err = SetCheckpoint2(this, aggtype, aggname, expected1)
	assert.NoError(t, err)
	// and reading it now, we get new value
	checkpoint, err = GetCheckpoint2(this, aggtype, aggname)
	assert.NoError(t, err)
	assert.Equal(t, expected1, checkpoint)

	//can reset it again
	expected2 := ftypes.OidType(2)
	err = SetCheckpoint2(this, aggtype, aggname, expected2)
	assert.NoError(t, err)
	checkpoint, err = GetCheckpoint2(this, aggtype, aggname)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)

	// meanwhile other counter types aren't affected
	aggname2 := ftypes.AggName("other counter")
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err = GetCheckpoint2(this, aggtype, aggname2)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	expected3 := ftypes.OidType(51)
	err = SetCheckpoint2(this, aggtype, aggname2, expected3)
	assert.NoError(t, err)

	checkpoint, err = GetCheckpoint2(this, aggtype, aggname2)
	assert.NoError(t, err)
	assert.Equal(t, expected3, checkpoint)

	// meanwhile checkpoint for original CT isn't affected
	checkpoint, err = GetCheckpoint2(this, aggtype, aggname)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)
}
