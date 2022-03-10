package profile

import (
	"context"
	"fennel/db"
	"fennel/lib/profile"
	"fennel/lib/value"
	"testing"

	"fennel/lib/ftypes"
	"fennel/test"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
)

type mockProvider struct {
	ret []byte
}

func (m *mockProvider) change(n []byte) {
	m.ret = n
}
func (m *mockProvider) set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	return nil
}
func (m *mockProvider) get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	return m.ret, nil
}

var _ provider = &mockProvider{}

func TestCachedDBBasic(t *testing.T) {
	testProviderBasic(t, cachedProvider{base: dbProvider{}})
}

func TestCaching(t *testing.T) {
	// test that we cache the value instead of always pulling from ground truth
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	origmock := []byte{1, 2, 3}
	gt := mockProvider{origmock}
	p := cachedProvider{base: &gt}
	//p := CachedDB{cache: redis.NewCache(client.(redis.Client)), groundTruth: &gt}
	//err = p.Init()
	//assert.NoError(t, err)

	// initially we should get the mocked origmock value back
	found, err := p.get(ctx, tier, "1", 1232, "summary", 1)
	assert.NoError(t, err)
	assert.Equal(t, origmock, found)

	// now change the mocked value
	newmock := []byte{4, 5}
	gt.change(newmock)

	// we should still get origmock back because it's in cache
	found, err = p.get(ctx, tier, "1", 1232, "summary", 1)
	assert.NoError(t, err)
	assert.Equal(t, origmock, found)

	// but if we set a new value, we will delete the key (remember: we don't fill cache on sets)
	err = p.set(ctx, tier, "1", 1232, "summary", 1, []byte{7, 8, 9})
	assert.NoError(t, err)

	// so subsequent gets should get the new updated mock back
	found, err = p.get(ctx, tier, "1", 1232, "summary", 1)
	assert.Equal(t, newmock, found)
}

func TestCachedDBVersion(t *testing.T) {
	testProviderVersion(t, cachedProvider{base: dbProvider{}})
}

func TestCachedGetBatch(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := cachedProvider{base: dbProvider{}}

	expected1, _ := value.Marshal(value.Int(1))
	expected2, _ := value.Marshal(value.Int(2))
	expected3, _ := value.Marshal(value.Int(3))
	expected4 := expected3

	profile1 := profile.NewProfileItem("1", 1232, "summary", 1)
	profile2 := profile.NewProfileItem("2", 1232, "something else", 1)

	// same as one but with different version
	profile3 := profile.NewProfileItem("1", 1232, "summary", 5)
	// same as three but version set to zero
	profile4 := profile.NewProfileItem("1", 1232, "summary", 0)
	// initially all are empty
	found, err := p.getBatched(ctx, tier, []profile.ProfileItem{profile1, profile2, profile3, profile4})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{nil, nil, nil, nil}, found)

	// do a bunch of sets
	assert.NoError(t, p.set(ctx, tier, profile1.OType, profile1.Oid, profile1.Key, profile1.Version, expected1))
	assert.NoError(t, p.set(ctx, tier, profile2.OType, profile2.Oid, profile2.Key, profile2.Version, expected2))
	assert.NoError(t, p.set(ctx, tier, profile3.OType, profile3.Oid, profile3.Key, profile3.Version, expected3))

	found, err = p.getBatched(ctx, tier, []profile.ProfileItem{profile1, profile2, profile3, profile4})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{expected1, expected2, expected3, expected4}, found)

	// now that everything should be in cache, we will "disable" db and verify that it still works
	tier.DB = db.Connection{}
	found, err = p.getBatched(ctx, tier, []profile.ProfileItem{profile1, profile2, profile3, profile4})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{expected1, expected2, expected3, expected4}, found)
}
