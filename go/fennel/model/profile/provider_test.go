package profile

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"
	"fennel/tier"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testProviderBasic(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	val := value.Int(2)
	expected, _ := value.Marshal(val)

	// initially before setting, value isn't there so we get nil back
	// and calling get on a row that doesn't exist is not an error
	profile1 := profile.NewProfileItemSer("1", 1232, "summary", 1, expected)
	checkGet(t, ctx, p, tier, profile1, []byte(nil))
	checkGetVersion(t, ctx, p, tier, "1", 1232, "summary", 0)

	// and repeating this should be same (vs any cache issues)
	checkGet(t, ctx, p, tier, profile1, []byte(nil))
	checkGetVersion(t, ctx, p, tier, "1", 1232, "summary", 0)

	// now set the value
	checkSet(t, ctx, p, tier, profile1, expected)

	// now get the same value back
	checkGet(t, ctx, p, tier, profile1, expected)
	checkGetVersion(t, ctx, p, tier, "1", 1232, "summary", 1)

	// and get it again to verify nothing changes
	checkGet(t, ctx, p, tier, profile1, expected)
	checkGetVersion(t, ctx, p, tier, "1", 1232, "summary", 1)

	// test getMulti now
	request := profile.ProfileFetchRequest{}
	checkMultiGet(t, ctx, tier, request, []profile.ProfileItemSer{profile1})
}

func testProviderVersion(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	profiles := make([]profile.ProfileItemSer, 0)
	request := profile.ProfileFetchRequest{}

	// initially table is empty
	checkMultiGet(t, ctx, tier, request, profiles)

	val1 := value.Int(2)
	expected1, _ := value.Marshal(val1)

	// first setting a version of 0 isn't possible
	err = p.set(ctx, tier, "1", 1232, "summary", 0, expected1)
	assert.Error(t, err)

	// but it works with a valid version
	profiles = append(profiles, profile.NewProfileItemSer("1", 1232, "summary", 1, expected1))
	checkSet(t, ctx, p, tier, profiles[0], expected1)
	checkMultiGet(t, ctx, tier, request, profiles)

	// and can set another version on the same value
	val2 := value.String("hello")
	expected2, _ := value.Marshal(val2)
	profiles = append(profiles, profile.NewProfileItemSer("1", 1232, "summary", 2, expected2))
	checkSet(t, ctx, p, tier, profiles[1], expected2)
	checkMultiGet(t, ctx, tier, request, profiles)

	// versions can also be non-continuous
	val3 := value.Dict(map[string]value.Value{
		"hi":  value.Int(1),
		"bye": value.List([]value.Value{value.Bool(true), value.String("yo")}),
	})
	expected3, _ := value.Marshal(val3)
	profiles = append(profiles, profile.NewProfileItemSer("1", 1232, "summary", 10, expected3))
	checkSet(t, ctx, p, tier, profiles[2], expected3)
	checkMultiGet(t, ctx, tier, request, profiles)

	// we can get any of these versions back
	checkGet(t, ctx, p, tier, profiles[0], expected1)
	checkGetVersion(t, ctx, p, tier, "1", 1232, "summary", 10)
	checkGet(t, ctx, p, tier, profiles[1], expected2)
	checkGetVersion(t, ctx, p, tier, "1", 1232, "summary", 10)
	checkGet(t, ctx, p, tier, profiles[2], expected3)
	checkGetVersion(t, ctx, p, tier, "1", 1232, "summary", 10)

	// if we ask for version 0, by default get the highest version
	found, err := p.get(ctx, tier, "1", 1232, "summary", 0)
	assert.NoError(t, err)
	assert.Equal(t, expected3, found)

	// and asking for a version that doesn't exist return empty string
	found, err = p.get(ctx, tier, "1", 1232, "summary", 5)
	assert.NoError(t, err)
	assert.Equal(t, []byte(nil), found)
}

func checkSet(t *testing.T, ctx context.Context, p provider, tier tier.Tier, pi profile.ProfileItemSer, valueSer []byte) {
	err := p.set(ctx, tier, pi.OType, pi.Oid, pi.Key, pi.Version, valueSer)
	assert.NoError(t, err)
}

func checkGet(t *testing.T, ctx context.Context, p provider, tier tier.Tier, pi profile.ProfileItemSer, expected []byte) {
	found, err := p.get(ctx, tier, pi.OType, pi.Oid, pi.Key, pi.Version)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func checkMultiGet(t *testing.T, ctx context.Context, tier tier.Tier, request profile.ProfileFetchRequest, expected []profile.ProfileItemSer) {
	found, err := GetMulti(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, found)
}

func checkGetVersion(t *testing.T, ctx context.Context, p provider, tier tier.Tier, otype ftypes.OType, oid uint64, key string, expectedv uint64) {
	found, err := p.getversion(ctx, tier, otype, oid, key)
	assert.NoError(t, err)
	assert.Equal(t, expectedv, found)
}
