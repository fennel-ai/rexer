//go:build integration

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
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestCachedGetBatchMultipleObjs(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := cachedProvider{base: dbProvider{}}

	profile1 := profile.NewProfileItem("1", "1232", "summary", value.Int(1), 1)

	// same as one but with different version
	profile2 := profile.NewProfileItem("2", "1232", "something else ", value.Int(3), 0)
	// same as one but version set to zero
	profile3 := profile.NewProfileItem("1", "1232", "summary", value.Int(5), 2)

	// different object
	profile4 := profile.NewProfileItem("1", "1232", "summary", value.Int(7), 3)
	// initially all are empty
	keys := []profile.ProfileItemKey{profile1.GetProfileKey(), profile2.GetProfileKey(), profile3.GetProfileKey(), profile4.GetProfileKey()}
	found, err := p.getBatch(ctx, tier, keys)
	assert.NoError(t, err)
	emptyProf := profile.NewProfileItem("1", "1232", "summary", value.Nil, 0)
	emptyProf2 := profile.NewProfileItem("2", "1232", "something else ", value.Nil, 0)

	assert.Equal(t, []profile.ProfileItem{emptyProf, emptyProf2, emptyProf, emptyProf}, found)

	// do a bunch of sets
	assert.NoError(t, p.set(ctx, tier, profile1))
	assert.NoError(t, p.set(ctx, tier, profile2))
	assert.NoError(t, p.set(ctx, tier, profile3))
	found, err = p.getBatch(ctx, tier, keys)
	//assert.NoError(t, err)
	profile2.UpdateTime = 0
	profile3.UpdateTime = 0

	assert.Equal(t, []profile.ProfileItem{profile3, profile2, profile3, profile3}, found)

	// now that everything should be in cache, we will "disable" db and verify that it still works
	tier.DB = db.Connection{}
	found, err = p.getBatch(ctx, tier, keys)
	assert.NoError(t, err)
	assert.Equal(t, []profile.ProfileItem{profile3, profile2, profile3, profile3}, found)
}

func TestCachedDBConcurrentMultiSet(t *testing.T) {
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
		for i := 0; i < 5; i++ {
			go func(i int) {
				defer wg.Done()
				defer wg.Done()
				assert.NoError(t, c.setBatch(ctx, tier, []profile.ProfileItem{
					profiles[i*2], profiles[i*2+1],
				}))
			}(i)
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

	// ("user", 0, "age", 9) would be the lastest profile
	pk := profile.NewProfileItemKey("user", "1", "age")
	v, err := tier.Cache.Get(ctx, makeKey(pk))
	assert.NoError(t, err)
	// ("user", 1, "age", 10) would be the lastest profile
	expectedv := value.ToJSON(value.NewList(value.Int(8)))
	assert.Equal(t, expectedv, []byte(v.(string)))
}

// tests that the cache is atleast eventually consistent (it is not easy to test intermediate states)
//
// we do so by assuming multiple cache evictions have taken place (only DB entries exist), perform
// concurrent reads and writes for the profiles and set expectations on value stored for "latest profile"
func TestCachedDBEventuallyConsistentMultipleObjs(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	db := dbProvider{}
	c := cachedProvider{base: db}

	// creates versioned profiles for ("user", 0, "age") and ("user", 1, "age")
	p := make([]profile.ProfileItem, 0)
	for i := uint64(1); i <= 10; i++ {
		v := value.NewList(value.Int(i))
		p = append(p, profile.ProfileItem{OType: "user", Oid: ftypes.OidType(strconv.FormatUint(i%2+1, 10)), Key: "age", UpdateTime: i, Value: v})
	}
	assert.NoError(t, c.setBatch(ctx, tier, p))

	// remove few entries from the cache - eviction
	// these could be random, for the sake of testing, picking few numbers..
	tier.Cache.Delete(ctx, []string{
		makeKey(profile.NewProfileItemKey("user", "1", "age")),
	}...)

	wg := sync.WaitGroup{}
	wg.Add(16)

	// read the latest version of the profiles simulatenously as they are updated
	//
	// versioned profiles are skipped here as they are covered above already:
	//  i) updating a versioned profile is not possible
	//  ii) if cache entry for a versioned profile is evicted, get/getbatched should update it
	go func() {
		for i := 0; i < 10; i++ {
			go func() {
				defer wg.Done()
				pbatch := make([]profile.ProfileItemKey, 0)
				// randomly sample profiles
				if rand.Intn(2) == 1 {
					pbatch = append(pbatch, profile.ProfileItemKey{
						OType: "user",
						Oid:   "1",
						Key:   "age",
					})
				}
				if rand.Intn(2) == 1 {
					pbatch = append(pbatch, profile.ProfileItemKey{
						OType: "user",
						Oid:   "2",
						Key:   "age",
					})
				}
				_, err := c.getBatch(ctx, tier, pbatch)
				// we do not assert on the read values because it is not deterministic
				assert.NoError(t, err)
			}()
		}
	}()

	// set new profiles, should update latest "versions" in the cache
	go func() {
		p := make([]profile.ProfileItem, 0)
		for i := uint64(1); i <= 3; i++ {
			defer wg.Done()
			v := value.NewList(value.Int(i * 20))
			p = append(p, profile.ProfileItem{OType: "user", Oid: "1", Key: "age", UpdateTime: i * 20, Value: v})
		}
		assert.NoError(t, c.setBatch(ctx, tier, p))
	}()
	go func() {
		p := make([]profile.ProfileItem, 0)
		for i := uint64(3); i >= 1; i-- {
			defer wg.Done()
			v := value.NewList(value.Int(i * 20))
			p = append(p, profile.ProfileItem{OType: "user", Oid: "2", Key: "age", UpdateTime: i * 20, Value: v})
		}
		assert.NoError(t, c.setBatch(ctx, tier, p))
	}()
	wg.Wait()

	// check that the latest profile can be accessed by provided version = 0
	// these should return values set as part of the second go routine above
	v, err := tier.Cache.Get(ctx, makeKey(profile.NewProfileItemKey("user", "1", "age")))
	assert.NoError(t, err)
	// ("user", 0, "age", 60) would be the lastest profile
	expectedv := value.ToJSON(value.NewList(value.Int(60)))
	assert.Equal(t, expectedv, []byte(v.(string)))

	v, err = tier.Cache.Get(ctx, makeKey(profile.NewProfileItemKey("user", "2", "age")))
	assert.NoError(t, err)
	// ("user", 1, "age", 60) would be the lastest profile
	assert.Equal(t, expectedv, []byte(v.(string)))
}
