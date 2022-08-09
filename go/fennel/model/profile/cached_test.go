package profile

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"fennel/db"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/sql"
	"fennel/lib/value"

	"fennel/test"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
)

type mockProvider struct {
	ret profile.ProfileItem
}

func (m *mockProvider) change(n profile.ProfileItem) {
	m.ret = n
}

func (m *mockProvider) set(ctx context.Context, tier tier.Tier, profile profile.ProfileItem) error {
	return nil
}

func (m *mockProvider) setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItem) error {
	m.change(profiles[0])
	return nil
}

func (m *mockProvider) get(ctx context.Context, tier tier.Tier, profileKey profile.ProfileItemKey) (profile.ProfileItem, error) {
	return m.ret, nil
}

func (m *mockProvider) getBatch(ctx context.Context, tier tier.Tier, profileKeys []profile.ProfileItemKey) ([]profile.ProfileItem, error) {
	mp := make([]profile.ProfileItem, 1)
	mp[0] = m.ret
	return mp, nil
}

func (m *mockProvider) query(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid ftypes.OidType, pagination sql.Pagination) ([]profile.ProfileItem, error) {
	mp := make([]profile.ProfileItem, 1)
	mp[0] = m.ret
	return mp, nil
}

// this is used only in `TestCaching`
var _ provider = &mockProvider{}

func TestCachedDBBasic(t *testing.T) {
	t.Parallel()
	provider := cachedProvider{base: dbProvider{}}
	t.Run("cache_db_basic", func(t *testing.T) {
		testProviderBasic(t, provider)
	})

	t.Run("cache_db_set_again", func(t *testing.T) {
		testSetAgain(t, provider)
	})
	// Mini-redis makes this test fail, since it requires all keys to be in same slot.
	// t.Run("cache_db_set_get_batch", func(t *testing.T) {
	// 	testSetGetBatch(t, provider)
	// })
	t.Run("cache_db_get_multi", func(t *testing.T) {
		testSQLGetMulti(t, provider)
	})

	t.Run("cached_db_query", func(t *testing.T) {
		testQuery(t, provider)
	})
}

func TestCaching(t *testing.T) {
	// test that we cache the value instead of always pulling from ground truth
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	origmock := profile.NewProfileItem("users", "1232", "gender", value.String("male"), 1)
	expected := profile.NewProfileItem("users", "1232", "gender", value.String("male"), 0)
	gt := mockProvider{origmock}
	p := cachedProvider{base: &gt}

	// initially we should get the mocked origmock value back
	found, err := p.get(ctx, tier, origmock.GetProfileKey())
	assert.NoError(t, err)
	assert.Equal(t, origmock, found)

	// now change the mocked value
	newmock := profile.NewProfileItem("users", "1232", "gender", value.String("female"), 1)
	gt.change(newmock)

	// we should still get origmock back because it's in cache
	found, err = p.get(ctx, tier, origmock.GetProfileKey())
	assert.NoError(t, err)
	assert.Equal(t, expected, found)

	// but if we set a new value, we update the cache as well.
	origmock.Value = value.String("unknown")
	origmock.UpdateTime = 2
	err = p.set(ctx, tier, origmock)
	assert.NoError(t, err)

	// so subsequent gets should get the new updated mock back
	found, err = p.get(ctx, tier, origmock.GetProfileKey())
	assert.NoError(t, err)
	expected.Value = value.String("unknown")
	assert.Equal(t, expected, found)
}

func TestCachedGetBatch(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := cachedProvider{base: dbProvider{}}

	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test behavior across
	// different objects in `_integration_test`
	pk := profile.NewProfileItemKey("user", "1", "age")
	prof := profile.NewProfileItem("user", "1", "age", value.Nil, 0)
	// initially all are empty
	found, err := p.getBatch(ctx, tier, []profile.ProfileItemKey{pk, pk, pk})
	assert.NoError(t, err)
	assert.Equal(t, []profile.ProfileItem{prof, prof, prof}, found)

	p1 := profile.NewProfileItem("user", "1", "age", value.Int(234), 0)
	p2 := profile.NewProfileItem("user", "2", "age", value.Int(3244), 123)
	// p2 but latest
	p3 := profile.NewProfileItem("user", "2", "age", value.Int(757), 156)

	// Reads without writes should return empty
	pks := []profile.ProfileItemKey{p1.GetProfileKey(), p2.GetProfileKey(), p3.GetProfileKey()}
	found, err = p.getBatch(ctx, tier, pks)
	assert.NoError(t, err)
	assert.Equal(t, []profile.ProfileItem{
		profile.NewProfileItem("user", "1", "age", value.Nil, 0),
		profile.NewProfileItem("user", "2", "age", value.Nil, 0),
		profile.NewProfileItem("user", "2", "age", value.Nil, 0),
	}, found)

	// do a bunch of sets
	assert.NoError(t, p.set(ctx, tier, p1))
	assert.NoError(t, p.set(ctx, tier, p2))
	assert.NoError(t, p.set(ctx, tier, p3))

	// Ask for profiles
	found, err = p.getBatch(ctx, tier, pks)
	assert.NoError(t, err)
	expectedProf3 := profile.NewProfileItem("user", "2", "age", value.Int(757), 0)
	assert.Equal(t, []profile.ProfileItem{p1, expectedProf3, expectedProf3}, found)

	// now that everything should be in cache, we will "disable" db and verify that it still works
	tier.DB = db.Connection{}
	found, err = p.getBatch(ctx, tier, pks)
	assert.NoError(t, err)
	assert.Equal(t, []profile.ProfileItem{p1, expectedProf3, expectedProf3}, found)
}

func TestCachedDBConcurrentSet(t *testing.T) {
	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test `setBatch` behavior across
	// different objects in `_integration_test`
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	c := cachedProvider{base: dbProvider{}}

	profiles := make([]profile.ProfileItem, 0)
	cacheKeys := make([]string, 0)
	for i := uint64(0); i < 10; i++ {
		p := profile.ProfileItem{
			OType:      "user",
			Oid:        ftypes.OidType(strconv.FormatUint(i%2+1, 10)),
			Key:        "age",
			UpdateTime: i + 1,
			Value:      value.NewList(value.Int(i)),
		}
		profiles = append(profiles, p)
		cacheKeys = append(cacheKeys, makeKey(p.GetProfileKey()))
	}

	wg := sync.WaitGroup{}
	wg.Add(10)
	go func() {
		// goroutine to write profile data
		for _, p := range profiles {
			go func(p profile.ProfileItem) {
				defer wg.Done()
				assert.NoError(t, c.set(ctx, tier, p))
			}(p)
		}
	}()
	wg.Wait()

	// check for all profiles are set in cache
	vs, err := tier.Cache.MGet(ctx, cacheKeys...)
	assert.NoError(t, err)
	for i, v := range vs {
		var expectedv []byte
		if i%2 == 0 {
			expectedv = value.ToJSON(value.NewList(value.Int(8)))
		} else {
			expectedv = value.ToJSON(value.NewList(value.Int(9)))
		}
		assert.Equal(t, expectedv, []byte(v.(string)))
	}

	// check that the latest profile can be accessed by provided version = 0
	v, err := tier.Cache.Get(ctx, makeKey(profile.NewProfileItemKey("user", "1", "age")))
	assert.NoError(t, err)
	// ("user", 0, "age", 9) would be the lastest profile
	expectedv := value.ToJSON(value.NewList(value.Int(8)))
	assert.Equal(t, expectedv, []byte(v.(string)))

	v, err = tier.Cache.Get(ctx, makeKey(profile.NewProfileItemKey("user", "2", "age")))
	assert.NoError(t, err)
	// ("user", 1, "age", 10) would be the lastest profile
	expectedv = value.ToJSON(value.NewList(value.Int(9)))
	assert.Equal(t, expectedv, []byte(v.(string)))
}

func TestCachedDBCacheMissOnReadSets(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	c := cachedProvider{base: dbProvider{}}

	prof := profile.NewProfileItem("user", "1", "age", value.Int(2), 4)
	assert.NoError(t, c.set(ctx, tier, prof))

	// explicitly delete the cache entry - eviction
	assert.NoError(t, tier.Cache.Delete(ctx, makeKey(profile.NewProfileItemKey("user", "1", "age"))))
	pk := prof.GetProfileKey()
	v, err := c.get(ctx, tier, pk)
	assert.NoError(t, err)
	prof.UpdateTime = 0
	assert.Equal(t, prof, v)

	// check that the cache was updated
	vinf, err := tier.Cache.Get(ctx, makeKey(pk))
	assert.NoError(t, err)
	assert.Equal(t, "2", vinf.(string))
}

// tests that the cache is atleast eventually consistent (it is not easy to test intermediate states)
//
// we do so by assuming multiple cache evictions have taken place (only DB entries exist), perform
// concurrent reads and writes for the profiles and set expectations on value stored for "latest profile"
func TestCachedDBEventuallyConsistent(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	db := dbProvider{}
	c := cachedProvider{base: db}

	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test `setBatch` behavior across
	// different objects in `_integration_test`

	// creates versioned profiles for ("user", i, "age")
	for i := uint64(1); i <= 5; i++ {
		assert.NoError(t, c.set(ctx, tier, profile.NewProfileItem("user", ftypes.OidType(strconv.FormatUint(i, 10)), "age", value.NewList(value.Int(i-1)), i-1)))
		assert.NoError(t, c.set(ctx, tier, profile.NewProfileItem("user", ftypes.OidType(strconv.FormatUint(i, 10)), "age", value.NewList(value.Int(i)), i)))
	}

	// remove few entries from the cache - eviction
	// these could be random, for the sake of testing, picking few numbers..
	_ = tier.Cache.Delete(ctx, []string{
		makeKey(profile.NewProfileItemKey("user", "2", "age")),
		makeKey(profile.NewProfileItemKey("user", "3", "age")),
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
			go func(i int) {
				defer wg.Done()
				pbatch := make([]profile.ProfileItemKey, 0)
				// randomly sample profiles
				if rand.Intn(2) == 1 {
					pbatch = append(pbatch, profile.NewProfileItemKey("user", ftypes.OidType(strconv.Itoa(i)), "age"))
				}
				_, err := c.getBatch(ctx, tier, pbatch)
				// we do not assert on the read values because it is not deterministic
				assert.NoError(t, err)
			}(i)
		}
	}()

	// set new profiles, should update latest "versions" in the cache
	go func() {
		for i := uint64(1); i <= 3; i++ {
			go func(i uint64) {
				defer wg.Done()
				assert.NoError(t, c.set(ctx, tier, profile.NewProfileItem("user", ftypes.OidType(strconv.FormatUint(i, 10)), "age", value.NewList(value.Int(i*20)), 0)))
			}(i)
		}
	}()
	wg.Wait()

	v, err := tier.Cache.Get(ctx, makeKey(profile.NewProfileItemKey("user", "3", "age")))
	assert.NoError(t, err)
	// ("user", 1, "age", 60) would be the lastest profile
	expectedv := value.ToJSON(value.NewList(value.Int(60)))
	assert.Equal(t, expectedv, []byte(v.(string)))
}
