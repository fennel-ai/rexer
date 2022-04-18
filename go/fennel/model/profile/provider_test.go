package profile

import (
	"context"
	"testing"

	"fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
)

func testProviderBasic(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	// initially before setting, value isn't there so we get nil back
	// and calling get on a row that doesn't exist is not an error
	profile1 := profile.NewProfileItem("users", 1232, "gender", value.String("male"), 1)
	profileKey := profile1.GetProfileKey()
	expctedProf := profile.NewProfileItem("users", 1232, "gender", value.Nil, 0)
	checkGet(t, ctx, p, tier, profileKey, expctedProf)

	// and repeating this should be same (vs any cache issues)
	checkGet(t, ctx, p, tier, profileKey, expctedProf)

	// now set the value
	checkSet(t, ctx, p, tier, profile1)

	// now get the same value back
	expctedProf.Value = value.String("male")
	checkGet(t, ctx, p, tier, profileKey, expctedProf)

	// and get it again to verify nothing changes
	checkGet(t, ctx, p, tier, profileKey, expctedProf)
}

func testSQLGetMulti(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	profile1 := profile.NewProfileItem("users", 1232, "gender", value.String("male"), 1)
	profile2 := profile.NewProfileItem("users", 3456, "gender", value.String("male"), 1)

	// initially before setting, value isn't there so we get nothing back
	checkGetBatch(t, ctx, p, tier, []profile.ProfileItemKey{}, []profile.ProfileItem{})
	// and calling get on a row that doesn't exist is not an error

	// now set the value and verify we can get it from the provider
	checkSet(t, ctx, p, tier, profile1)
	profile1.UpdateTime = 0
	checkGetBatch(t, ctx, p, tier, []profile.ProfileItemKey{profile1.GetProfileKey()}, []profile.ProfileItem{profile1})

	// set one more and verify that also works
	checkSet(t, ctx, p, tier, profile2)
	profile2.UpdateTime = 0
	checkGetBatch(t, ctx, p, tier, []profile.ProfileItemKey{profile1.GetProfileKey(), profile2.GetProfileKey()}, []profile.ProfileItem{profile1, profile2})
}

func checkSet(t *testing.T, ctx context.Context, p provider, tier tier.Tier, prof profile.ProfileItem) {
	err := p.set(ctx, tier, prof)
	assert.NoError(t, err)
}

func checkGet(t *testing.T, ctx context.Context, p provider, tier tier.Tier, pk profile.ProfileItemKey, expected profile.ProfileItem) {
	found, err := p.get(ctx, tier, pk)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func checkGetBatch(t *testing.T, ctx context.Context, p provider, tier tier.Tier, profileKeys []profile.ProfileItemKey, expected []profile.ProfileItem) {
	found, err := p.getBatch(ctx, tier, profileKeys)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, found)
}

func testSetAgain(t *testing.T, p provider) {
	// Setting the same profile twice with same version should be an error unless the value is identical
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	val1 := value.Int(2)
	val2 := value.Int(5)
	p1 := profile.NewProfileItem("12", 1, "attr", val1, 1)
	expP1 := profile.NewProfileItem("12", 1, "attr", val1, 0)

	assert.NoError(t, p.set(ctx, tier, p1))

	checkGet(t, ctx, p, tier, p1.GetProfileKey(), expP1)
	// if we set the same profile again with same version/value, it should go through
	assert.NoError(t, p.set(ctx, tier, p1))
	checkGet(t, ctx, p, tier, p1.GetProfileKey(), expP1)

	// but if we try to set the same profile to different value, it will go through but not update
	p2 := profile.NewProfileItem("12", 1, "attr", val2, 1)
	assert.NoError(t, p.set(ctx, tier, p2))

	// and in all of this, value is not changed
	checkGet(t, ctx, p, tier, p1.GetProfileKey(), expP1)

	// We need to update the timestamp to make it different
	p3 := profile.NewProfileItem("12", 1, "attr", val2, 22)
	assert.NoError(t, p.set(ctx, tier, p3))

	// the value should now change to p3
	p3.UpdateTime = 0
	checkGet(t, ctx, p, tier, p1.GetProfileKey(), p3)
}

func testSetGetBatch(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	val1 := value.Int(2)
	val2 := value.Int(5)
	val3 := value.Int(15)

	profiles := []profile.ProfileItem{
		profile.NewProfileItem("12", 1, "score", val1, 1),
		profile.NewProfileItem("12", 1, "score", val2, 15),
		profile.NewProfileItem("12", 1, "score", val3, 20),
		profile.NewProfileItem("12", 2, "score", val1, 15),
		profile.NewProfileItem("12", 3, "score", val3, 15),
	}

	assert.NoError(t, p.setBatch(ctx, tier, profiles))

	actual, _ := p.getBatch(ctx, tier, []profile.ProfileItemKey{profiles[0].GetProfileKey(),
		profiles[3].GetProfileKey(), profiles[4].GetProfileKey()})
	profiles[2].UpdateTime = 0
	profiles[3].UpdateTime = 0
	profiles[4].UpdateTime = 0

	assert.Equal(t, []profile.ProfileItem{profiles[2], profiles[3], profiles[4]}, actual)

	val1 = value.Int(29)
	val2 = value.Int(59)
	val3 = value.Int(159)
	profiles2 := []profile.ProfileItem{
		profile.NewProfileItem("12", 1, "score", val1, 200),
		profile.NewProfileItem("12", 2, "score", val2, 9),
		profile.NewProfileItem("12", 3, "score", val3, 150),
	}

	assert.NoError(t, p.setBatch(ctx, tier, profiles2))

	actual, _ = p.getBatch(ctx, tier, []profile.ProfileItemKey{profiles2[0].GetProfileKey(),
		profiles2[1].GetProfileKey(), profiles2[2].GetProfileKey()})

	profiles2[0].UpdateTime = 0
	profiles[3].UpdateTime = 0
	profiles2[2].UpdateTime = 0
	assert.Equal(t, []profile.ProfileItem{profiles2[0], profiles[3], profiles2[2]}, actual)

}
