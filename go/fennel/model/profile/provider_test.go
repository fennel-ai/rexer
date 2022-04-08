package profile

import (
	"context"
	"testing"

	"fennel/lib/ftypes"
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

	val := value.Int(2)
	expected := value.ToJSON(val)

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
}

func testSQLGetMulti(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	val := value.Int(2)
	expected := value.ToJSON(val)
	profile1 := profile.NewProfileItemSer("1", 1232, "summary", 1, expected)
	profile2 := profile.NewProfileItemSer("1", 4567, "summary", 1, expected)
	request := profile.ProfileFetchRequest{}

	// initially before setting, value isn't there so we get nothing back
	checkMultiGet(t, ctx, tier, request, []profile.ProfileItemSer{})
	// and calling get on a row that doesn't exist is not an error

	// now set the value and verify we can get it from the provider
	checkSet(t, ctx, p, tier, profile1, expected)
	checkMultiGet(t, ctx, tier, request, []profile.ProfileItemSer{profile1})

	// set one more and verify that also works
	checkSet(t, ctx, p, tier, profile2, expected)
	checkMultiGet(t, ctx, tier, request, []profile.ProfileItemSer{profile1, profile2})
}

func testProviderVersion(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	profiles := make([]profile.ProfileItemSer, 0)
	val1 := value.Int(2)
	expected1 := value.ToJSON(val1)

	// first setting a version of 0 isn't possible
	err = p.set(ctx, tier, "1", 1232, "summary", 0, expected1)
	assert.Error(t, err)

	// but it works with a valid version
	profiles = append(profiles, profile.NewProfileItemSer("1", 1232, "summary", 1, expected1))
	checkSet(t, ctx, p, tier, profiles[0], expected1)

	// and can set another version on the same value
	val2 := value.String("hello")
	expected2 := value.ToJSON(val2)
	profiles = append(profiles, profile.NewProfileItemSer("1", 1232, "summary", 2, expected2))
	checkSet(t, ctx, p, tier, profiles[1], expected2)

	// versions can also be non-continuous
	val3 := value.NewDict(map[string]value.Value{
		"hi":  value.Int(1),
		"bye": value.NewList(value.Bool(true), value.String("yo")),
	})
	expected3 := value.ToJSON(val3)
	profiles = append(profiles, profile.NewProfileItemSer("1", 1232, "summary", 10, expected3))
	checkSet(t, ctx, p, tier, profiles[2], expected3)

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

	// and asking for a version that doesn't exist returns nil
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
	v := versionIdentifier{otype: otype, oid: oid, key: key}
	found, err := p.getVersionBatched(ctx, tier, []versionIdentifier{v})
	assert.NoError(t, err)
	assert.Equal(t, expectedv, found[v])
}

func testSetAgain(t *testing.T, p provider) {
	// Setting the same profile twice with same version should be an error unless the value is identical
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	val1 := value.Int(2)
	v1 := value.ToJSON(val1)
	val2 := value.Int(5)
	v2 := value.ToJSON(val2)

	assert.NoError(t, p.set(ctx, tier, "12", 1, "age", 1, v1))
	checkGet(t, ctx, p, tier, profile.NewProfileItemSer("12", 1, "age", 1, v1), v1)
	// if we set the same profile again with same version/value, it should be an error
	assert.NoError(t, p.set(ctx, tier, "12", 1, "age", 1, v1))
	checkGet(t, ctx, p, tier, profile.NewProfileItemSer("12", 1, "age", 1, v1), v1)

	// but if we try to set the same profile to different value, it should be an error
	assert.Error(t, p.set(ctx, tier, "12", 1, "age", 1, v2))

	// and in all of this, value is not changed
	checkGet(t, ctx, p, tier, profile.NewProfileItemSer("12", 1, "age", 1, v1), v1)
}

func testSetBatch(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	val := value.Int(2)
	val1 := value.Int(5)
	v := value.ToJSON(val)
	v1 := value.ToJSON(val1)
	profiles := []profile.ProfileItemSer{
		{OType: "12", Oid: 1, Key: "age", Version: 1, Value: v},
		{OType: "12", Oid: 1, Key: "age", Version: 5, Value: v1},
	}

	assert.NoError(t, p.setBatch(ctx, tier, profiles))

	actual, _ := p.get(ctx, tier, "12", 1, "age", 1)
	assert.Equal(t, v, actual)
	actual1, _ := p.get(ctx, tier, "12", 1, "age", 5)
	assert.Equal(t, v1, actual1)
	actual2, _ := p.get(ctx, tier, "12", 1, "age", 0)
	assert.Equal(t, v1, actual2)
}

func testGetVersionBatched(t *testing.T, p provider) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	val := value.Int(2)
	val1 := value.Int(5)
	v := value.ToJSON(val)
	v1 := value.ToJSON(val1)
	profiles := []profile.ProfileItemSer{
		{OType: "12", Oid: 1, Key: "age", Version: 1, Value: v},
		{OType: "12", Oid: 1, Key: "age2", Version: 5, Value: v1},
	}

	assert.NoError(t, p.setBatch(ctx, tier, profiles))

	vid := versionIdentifier{otype: "12", oid: 1, key: "age"}
	vid1 := versionIdentifier{otype: "12", oid: 1, key: "age2"}
	// this does not exist, should not have an entry in the map returned as well
	vid2 := versionIdentifier{otype: "15", oid: 1, key: "age"}
	vMap, err := p.getVersionBatched(ctx, tier, []versionIdentifier{vid, vid1, vid2})
	assert.NoError(t, err)

	version1, found := vMap[vid]
	assert.True(t, found)
	assert.Equal(t, version1, uint64(1))

	version2, found := vMap[vid1]
	assert.True(t, found)
	assert.Equal(t, version2, uint64(5))

	_, found = vMap[vid2]
	assert.False(t, found)
}
