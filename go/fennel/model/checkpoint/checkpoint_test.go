package checkpoint

import (
	"context"
	"testing"

	"fennel/lib/ftypes"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestCheckpoint2(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	aggtype := ftypes.AggType("rolling_counter")
	aggname := ftypes.AggName("mycounter")
	zero := ftypes.IDType(0)
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err := Get(ctx, tier, aggtype, aggname)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	// now set a checkpoint
	expected1 := ftypes.IDType(1)
	err = Set(ctx, tier, aggtype, aggname, expected1)
	assert.NoError(t, err)
	// and reading it now, we get new value
	checkpoint, err = Get(ctx, tier, aggtype, aggname)
	assert.NoError(t, err)
	assert.Equal(t, expected1, checkpoint)

	//can reset it again
	expected2 := ftypes.IDType(2)
	err = Set(ctx, tier, aggtype, aggname, expected2)
	assert.NoError(t, err)
	checkpoint, err = Get(ctx, tier, aggtype, aggname)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)

	// meanwhile other counter types aren't affected
	aggname2 := ftypes.AggName("other counter")
	// initially no checkpoint is setup, so we should get 0
	checkpoint, err = Get(ctx, tier, aggtype, aggname2)
	assert.NoError(t, err)
	assert.Equal(t, zero, checkpoint)

	expected3 := ftypes.IDType(51)
	err = Set(ctx, tier, aggtype, aggname2, expected3)
	assert.NoError(t, err)

	checkpoint, err = Get(ctx, tier, aggtype, aggname2)
	assert.NoError(t, err)
	assert.Equal(t, expected3, checkpoint)

	// meanwhile checkpoint for original CT isn't affected
	checkpoint, err = Get(ctx, tier, aggtype, aggname)
	assert.NoError(t, err)
	assert.Equal(t, expected2, checkpoint)
}
