package profile

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"fennel/db"
	"fennel/lib/profile"
	"fennel/lib/value"

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
func (m *mockProvider) setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItemSer) error {
	return nil
}
func (m *mockProvider) get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	return m.ret, nil
}
func (m *mockProvider) getVersionBatched(ctx context.Context, tier tier.Tier, vids []versionIdentifier) (map[versionIdentifier]uint64, error) {
	mp := make(map[versionIdentifier]uint64)
	mp[vids[0]] = 1
	return mp, nil
}

// this is used only in `TestCaching`
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

	// but if we set a new value, we update the cache as well.
	err = p.set(ctx, tier, "1", 1232, "summary", 1, []byte{7, 8, 9})
	assert.NoError(t, err)

	// so subsequent gets should get the new updated mock back
	found, err = p.get(ctx, tier, "1", 1232, "summary", 1)
	assert.NoError(t, err)
	assert.Equal(t, []byte{7, 8, 9}, found)
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

	expected1 := value.ToJSON(value.Int(1))
	expected2 := value.ToJSON(value.Int(3))
	expected3 := expected2

	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test behavior across
	// different objects in `_integration_test`
	profile1 := profile.NewProfileItem("1", 1232, "summary", 1)

	// same as one but with different version
	profile2 := profile.NewProfileItem("1", 1232, "summary", 5)
	// same as three but version set to zero
	profile3 := profile.NewProfileItem("1", 1232, "summary", 0)
	// initially all are empty
	found, err := p.getBatched(ctx, tier, []profile.ProfileItem{profile1, profile2, profile3})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{nil, nil, nil}, found)

	// do a bunch of sets
	assert.NoError(t, p.set(ctx, tier, profile1.OType, profile1.Oid, profile1.Key, profile1.Version, expected1))
	assert.NoError(t, p.set(ctx, tier, profile2.OType, profile2.Oid, profile2.Key, profile2.Version, expected2))

	// Ask for duplicate profiles
	found, err = p.getBatched(ctx, tier, []profile.ProfileItem{profile1, profile2, profile3, profile3})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{expected1, expected2, expected3, expected3}, found)

	// now that everything should be in cache, we will "disable" db and verify that it still works
	tier.DB = db.Connection{}
	found, err = p.getBatched(ctx, tier, []profile.ProfileItem{profile1, profile2, profile3, profile3})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{expected1, expected2, expected3, expected3}, found)
}

func TestCachedDBConcurrentSet(t *testing.T) {
	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test `setBatch` behavior across
	// different objects in `_integration_test`
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	c := cachedProvider{base: dbProvider{}}

	profiles := make([]profile.ProfileItem, 0)
	cacheKeys := make([]string, 0)
	for i := uint64(0); i < 10; i++ {
		p := profile.ProfileItem{
			OType:   "user",
			Oid:     i % 2,
			Key:     "age",
			Version: i + 1,
			Value:   value.NewList(value.Int(i)),
		}
		profiles = append(profiles, p)
		cacheKeys = append(cacheKeys, makeKey(p.OType, p.Oid, p.Key, p.Version))
	}

	wg := sync.WaitGroup{}
	wg.Add(10)
	go func() {
		// goroutine to write profile data
		for _, p := range profiles {
			go func(p profile.ProfileItem) {
				defer wg.Done()
				v := value.ToJSON(p.Value)
				assert.NoError(t, c.set(ctx, tier, p.OType, p.Oid, p.Key, p.Version, v))
			}(p)
		}
	}()
	wg.Wait()

	// check for all profiles are set in cache
	vs, err := tier.Cache.MGet(ctx, cacheKeys...)
	assert.NoError(t, err)
	for i, v := range vs {
		expectedv := value.ToJSON(value.NewList(value.Int(i)))
		assert.Equal(t, expectedv, []byte(v.(string)))
	}

	// check that the latest profile can be accessed by provided version = 0
	v, err := tier.Cache.Get(ctx, makeKey("user", 0, "age", 0))
	assert.NoError(t, err)
	// ("user", 0, "age", 9) would be the lastest profile
	expectedv := value.ToJSON(value.NewList(value.Int(8)))
	assert.Equal(t, expectedv, []byte(v.(string)))

	v, err = tier.Cache.Get(ctx, makeKey("user", 1, "age", 0))
	assert.NoError(t, err)
	// ("user", 1, "age", 10) would be the lastest profile
	expectedv = value.ToJSON(value.NewList(value.Int(9)))
	assert.Equal(t, expectedv, []byte(v.(string)))
}

func TestCachedDBCacheMissOnReadSets(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	c := cachedProvider{base: dbProvider{}}

	assert.NoError(t, c.set(ctx, tier, "user", 1, "age", 4, []byte{2}))

	// explicitly delete the cache entry - eviction
	assert.NoError(t, tier.Cache.Delete(ctx, makeKey("user", 1, "age", 4)))

	v, err := c.get(ctx, tier, "user", 1, "age", 4)
	assert.NoError(t, err)
	assert.Equal(t, []byte{2}, v)

	// check that the cache was updated
	vinf, err := tier.Cache.Get(ctx, makeKey("user", 1, "age", 4))
	assert.NoError(t, err)
	assert.Equal(t, []byte{2}, []byte(vinf.(string)))
}

// tests that the cache is atleast eventually consistent (it is not easy to test intermediate states)
//
// we do so by assuming multiple cache evictions have taken place (only DB entries exist), perform
// concurrent reads and writes for the profiles and set expectations on value stored for "latest profile"
func TestCachedDBEventuallyConsistent(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	db := dbProvider{}
	c := cachedProvider{base: db}

	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test `setBatch` behavior across
	// different objects in `_integration_test`

	// creates versioned profiles for ("user", 1, "age")
	for i := uint64(1); i <= 5; i++ {
		v := value.ToJSON(value.NewList(value.Int(i)))
		assert.NoError(t, c.set(ctx, tier, "user", 1, "age", i, v))
	}

	// remove few entries from the cache - eviction
	// these could be random, for the sake of testing, picking few numbers..
	tier.Cache.Delete(ctx, []string{
		makeKey("user", 1, "age", 4),
		makeKey("user", 1, "age", 0), // removes latest
	}...)

	wg := sync.WaitGroup{}
	wg.Add(8)

	// read the latest version of the profiles simulatenously as they are updated
	//
	// versioned profiles are skipped here as they are covered above already:
	//  i) updating a versioned profile is not possible
	//  ii) if cache entry for a versioned profile is evicted, get/getbatched should update it
	go func() {
		for i := 0; i < 5; i++ {
			go func() {
				defer wg.Done()
				pbatch := make([]profile.ProfileItem, 0)
				// randomly sample profiles
				if rand.Intn(2) == 1 {
					pbatch = append(pbatch, profile.ProfileItem{
						OType:   "user",
						Oid:     1,
						Key:     "age",
						Version: 0,
					})
				}
				_, err := c.getBatched(ctx, tier, pbatch)
				// we do not assert on the read values because it is not deterministic
				assert.NoError(t, err)
			}()
		}
	}()

	// set new profiles, should update latest "versions" in the cache
	go func() {
		for i := uint64(1); i <= 3; i++ {
			go func(i uint64) {
				defer wg.Done()
				v := value.ToJSON(value.NewList(value.Int(i * 20)))
				assert.NoError(t, c.set(ctx, tier, "user", 1, "age", i*20, v))
			}(i)
		}
	}()
	wg.Wait()

	v, err := tier.Cache.Get(ctx, makeKey("user", 1, "age", 0))
	assert.NoError(t, err)
	// ("user", 1, "age", 60) would be the lastest profile
	expectedv := value.ToJSON(value.NewList(value.Int(60)))
	assert.Equal(t, expectedv, []byte(v.(string)))
}
