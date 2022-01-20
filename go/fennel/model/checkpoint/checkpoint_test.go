package checkpoint

import (
	"fennel/data/lib"
	lib2 "fennel/profile/lib"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckpoint(t *testing.T) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)
	ct1 := lib.CounterType_USER_LIKE
	zero := lib2.OidType(0)
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err := GetCheckpoint(this, ct1)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	// now set a checkpoint
	expected1 := lib2.OidType(1)
	err = SetCheckpoint(this, ct1, expected1)
	assert.NoError(t, err)
	// and reading it now, we get new value
	checkpoint, err = GetCheckpoint(this, ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected1, checkpoint)

	//can reset it again
	expected2 := lib2.OidType(2)
	err = SetCheckpoint(this, ct1, expected2)
	assert.NoError(t, err)
	checkpoint, err = GetCheckpoint(this, ct1)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)

	// meanwhile other counter types aren't affected
	var ct2 lib.CounterType = lib.CounterType_USER_SHARE
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err = GetCheckpoint(this, ct2)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	expected3 := lib2.OidType(51)
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
