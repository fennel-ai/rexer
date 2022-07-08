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

func TestGetBatchDiffObjs(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	vals := []value.Value{value.Int(1), value.Int(2), value.Int(3)}
	profiles := []profilelib.ProfileItem{
		{OType: "User", Oid: "1", Key: "summary", UpdateTime: 1, Value: vals[0]},
		{OType: "User", Oid: "2", Key: "summary", UpdateTime: 1, Value: vals[1]},
		{OType: "User", Oid: "3", Key: "summary", UpdateTime: 1, Value: vals[2]},
	}
	pks := make([]profilelib.ProfileItemKey, 0, 3)
	for _, p := range profiles {
		pks = append(pks, p.GetProfileKey())
	}

	// initially nothing exists
	found, err := GetBatch(ctx, tier, pks)
	assert.NoError(t, err)
	p1 := profilelib.NewProfileItem("User", "1", "summary", value.Nil, 0)
	p2 := profilelib.NewProfileItem("User", "2", "summary", value.Nil, 0)
	p3 := profilelib.NewProfileItem("User", "3", "summary", value.Nil, 0)

	assert.Equal(t, []profilelib.ProfileItem{p1, p2, p3}, found)

	// set a few
	checkTestSet(t, ctx, tier, profiles[0])
	checkTestSet(t, ctx, tier, profiles[1])
	checkTestSet(t, ctx, tier, profiles[2])

	profiles[0].UpdateTime = 0
	profiles[1].UpdateTime = 0
	profiles[2].UpdateTime = 0
	found, err = GetBatch(ctx, tier, pks)
	assert.NoError(t, err)
	assert.Equal(t, profiles, found)
}

func TestProfileDBInsertDiffObjs(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	vals := []value.Value{value.Int(1), value.Int(2), value.Int(3), value.Int(4)}
	profiles := []profilelib.ProfileItem{
		{OType: "User", Oid: "122", Key: "summary", UpdateTime: 1, Value: vals[0]},
		{OType: "Userx", Oid: "1224", Key: "summary 2", UpdateTime: 10, Value: vals[1]},
		{OType: "User1", Oid: "1229", Key: "summary 3", UpdateTime: 12, Value: vals[2]},
		{OType: "User", Oid: "122", Key: "summary", UpdateTime: 11, Value: vals[3]},
	}
	assert.NoError(t, setBatch(ctx, tier, profiles))
	pks := make([]profilelib.ProfileItemKey, 0, 3)
	for _, p := range profiles {
		pks = append(pks, p.GetProfileKey())
	}
	// check that the entries were written
	actual, err := GetBatch(ctx, tier, pks)
	assert.NoError(t, err)
	profiles[1].UpdateTime = 0
	profiles[2].UpdateTime = 0
	profiles[3].UpdateTime = 0

	assert.ElementsMatch(t, []profilelib.ProfileItem{profiles[3], profiles[1], profiles[2], profiles[3]}, actual)

	// check for latest, versioned profiled
	v, err := GetBatch(ctx, tier, []profilelib.ProfileItemKey{
		{OType: "User", Oid: "122", Key: "summary"},
		{OType: "Userx", Oid: "1224", Key: "summary 2"},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []profilelib.ProfileItem{profiles[3], profiles[1]}, v)
}
