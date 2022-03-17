//go:build integration

package profile

import (
	"context"
	"testing"

	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestGetBatchedDiffObjs(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

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

func TestProfileDBInsertDiffObjs(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	vals := []value.Value{value.Int(1), value.Int(2), value.Int(3), value.Int(4)}
	profiles := []profilelib.ProfileItem{
		{OType: "User", Oid: 122, Key: "summary", Version: 1, Value: vals[0]},
		{OType: "Userx", Oid: 1224, Key: "summary 2", Version: 10, Value: vals[1]},
		{OType: "User1", Oid: 1229, Key: "summary 3", Version: 12, Value: vals[2]},
		{OType: "User", Oid: 122, Key: "summary", Version: 11, Value: vals[3]},
	}
	assert.NoError(t, dbInsert(ctx, tier, profiles))

	// check that the entries were written
	actual, err := GetBatched(ctx, tier, profiles)
	assert.NoError(t, err)
	assert.ElementsMatch(t, actual, vals)

	// check for latest, versioned profiled
	v, err := GetBatched(ctx, tier, []profilelib.ProfileItem{
		{OType: "User", Oid: 122, Key: "summary", Version: 0},
		{OType: "Userx", Oid: 1224, Key: "summary 2", Version: 0},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{vals[3], vals[1]}, v)
}
