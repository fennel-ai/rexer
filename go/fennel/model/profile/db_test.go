package profile

import (
	"context"
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestDBBasic(t *testing.T) {
	testProviderBasic(t, dbProvider{})
}

func TestDBVersion(t *testing.T) {
	testProviderVersion(t, dbProvider{})
}

func TestLongKey(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	p := dbProvider{}

	val := value.Int(2)
	expected := value.ToJSON(val)

	// can not set value on a makeKey that is greater than 255 chars
	err = p.set(ctx, tier, "1", 1232, utils.RandString(256), 1, expected)
	assert.Error(t, err)

	// but works for a makeKey of size upto 255
	err = p.set(ctx, tier, "1", 1232, utils.RandString(255), 1, expected)
	assert.NoError(t, err)
}

func TestLongOType(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := dbProvider{}

	val := value.Int(5)
	expected := value.ToJSON(val)

	// otype cannot be longer than 255 chars
	err = p.set(ctx, tier, ftypes.OType(utils.RandString(256)), 23, "key", 1, expected)
	assert.Error(t, err)

	// but works for otype of length 255 chars
	err = p.set(ctx, tier, ftypes.OType(utils.RandString(255)), 23, "key", 1, expected)
	assert.NoError(t, err)
}

func TestMultiSet(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := dbProvider{}

	val := value.Int(2)
	v := value.ToJSON(val)
	val1 := value.Int(5)
	v1 := value.ToJSON(val1)

	assert.NoError(t, p.set(ctx, tier, "12", 1, "age", 1, v))
	// write the same profile should not fail the call, nor should it
	// update the existing value
	assert.NoError(t, p.set(ctx, tier, "12", 1, "age", 1, v1))

	actual, _ := p.get(ctx, tier, "12", 1, "age", 1)
	assert.Equal(t, v, actual)
}

func TestSetBatch(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := dbProvider{}

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

func TestGetVersionBatched(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := dbProvider{}

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
