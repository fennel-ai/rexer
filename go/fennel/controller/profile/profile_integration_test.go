//go:build integration

package profile

import (
	"context"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetBatchedDiffObjs(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test behavior across
	// different objects in `_integration_test`
	vals := []value.Value{value.Int(1), value.Int(2), value.Int(3)}
	profiles := []profilelib.ProfileItem{
		{OType: "User", Oid: uint64(1), Key: "summary", Version: 1, Value: vals[0]},
		{OType: "User", Oid: uint64(2), Key: "summary", Version: 1, Value: vals[1]},
		{OType: "User", Oid: uint64(3), Key: "summary", Version: 1, Value: vals[2]},
	}

	// initially nothing exists
	found, err := GetBatched(ctx, tier, profiles)
	assert.NoError(t, err)
	assert.Equal(t, []value.Value{nil, nil, nil}, found)

	// set a few
	checkSet(t, ctx, tier, profiles[0])
	checkSet(t, ctx, tier, profiles[1])
	checkSet(t, ctx, tier, profiles[2])

	found, err = GetBatched(ctx, tier, profiles)
	assert.NoError(t, err)
	assert.Equal(t, vals, found)
}
