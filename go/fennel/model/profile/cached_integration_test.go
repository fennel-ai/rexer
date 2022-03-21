//go:build integration

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
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestCachedGetBatchMultipleObjs(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := cachedProvider{base: dbProvider{}}

	expected1 := value.ToJSON(value.Int(1))
	expected2 := value.ToJSON(value.Int(3))
	expected3 := value.ToJSON(value.Int(5))
	expected4 := expected3

	profile1 := profile.NewProfileItem("1", 1232, "summary", 1)

	// same as one but with different version
	profile2 := profile.NewProfileItem("2", 1232, "something else ", 1)
	// same as one but version set to zero
	profile3 := profile.NewProfileItem("1", 1232, "summary", 5)

	// different object
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

func TestCachedDBConcurrentMultiSet(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	c := cachedProvider{base: dbProvider{}}

	profiles := make([]profile.ProfileItemSer, 0)
	cacheKeys := make([]string, 0)
	for i := uint64(0); i < 10; i++ {
		v := value.ToJSON(value.NewList(value.Int(i)))
		p := profile.ProfileItemSer{
			OType:   "user",
			Oid:     i % 2,
			Key:     "age",
			Version: i + 1,
			Value:   v,
		}
		profiles = append(profiles, p)
		cacheKeys = append(cacheKeys, makeKey(p.OType, p.Oid, p.Key, p.Version))
	}

	wg := sync.WaitGroup{}
	wg.Add(10)
	go func() {
		// goroutine to write profile data
		for i := 0; i < 5; i++ {
			go func(i int) {
				defer wg.Done()
				defer wg.Done()
				assert.NoError(t, c.setBatch(ctx, tier, []profile.ProfileItemSer{
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
<<<<<<< HEAD
		expectedv := value.ToJSON(value.List{value.Int(i)})
=======
		expectedv, _ := value.Marshal(value.NewList(value.Int(i)))
>>>>>>> a76d697 (value: hide List/Dict behind struct vs naked typedef; disallow nested lists)
		assert.Equal(t, expectedv, []byte(v.(string)))
	}

	// check that the latest profile can be accessed by provided version = 0
	v, err := tier.Cache.Get(ctx, makeKey("user", 0, "age", 0))
	assert.NoError(t, err)
	// ("user", 0, "age", 9) would be the lastest profile
<<<<<<< HEAD
	expectedv := value.ToJSON(value.List{value.Int(8)})
=======
	expectedv, _ := value.Marshal(value.NewList(value.Int(8)))
>>>>>>> a76d697 (value: hide List/Dict behind struct vs naked typedef; disallow nested lists)
	assert.Equal(t, expectedv, []byte(v.(string)))

	v, err = tier.Cache.Get(ctx, makeKey("user", 1, "age", 0))
	assert.NoError(t, err)
	// ("user", 1, "age", 10) would be the lastest profile
<<<<<<< HEAD
	expectedv = value.ToJSON(value.List{value.Int(9)})
=======
	expectedv, _ = value.Marshal(value.NewList(value.Int(9)))
>>>>>>> a76d697 (value: hide List/Dict behind struct vs naked typedef; disallow nested lists)
	assert.Equal(t, expectedv, []byte(v.(string)))
}

// tests that the cache is atleast eventually consistent (it is not easy to test intermediate states)
//
// we do so by assuming multiple cache evictions have taken place (only DB entries exist), perform
// concurrent reads and writes for the profiles and set expectations on value stored for "latest profile"
func TestCachedDBEventuallyConsistentMultipleObjs(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	db := dbProvider{}
	c := cachedProvider{base: db}

	// creates versioned profiles for ("user", 0, "age") and ("user", 1, "age")
	p := make([]profile.ProfileItemSer, 0)
	for i := uint64(1); i <= 10; i++ {
<<<<<<< HEAD
		v := value.ToJSON(value.List{value.Int(i)})
=======
		v, _ := value.Marshal(value.NewList(value.Int(i)))
>>>>>>> a76d697 (value: hide List/Dict behind struct vs naked typedef; disallow nested lists)
		p = append(p, profile.ProfileItemSer{OType: "user", Oid: i % 2, Key: "age", Version: i, Value: v})
	}
	assert.NoError(t, c.setBatch(ctx, tier, p))

	// remove few entries from the cache - eviction
	// these could be random, for the sake of testing, picking few numbers..
	tier.Cache.Delete(ctx, []string{
		makeKey("user", 0, "age", 4),
		makeKey("user", 0, "age", 0), // removes latest
		makeKey("user", 1, "age", 5),
		makeKey("user", 1, "age", 7),
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
				pbatch := make([]profile.ProfileItem, 0)
				// randomly sample profiles
				if rand.Intn(2) == 1 {
					pbatch = append(pbatch, profile.ProfileItem{
						OType:   "user",
						Oid:     0,
						Key:     "age",
						Version: 0,
					})
				}
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
		p := make([]profile.ProfileItemSer, 0)
		for i := uint64(1); i <= 3; i++ {
			defer wg.Done()
<<<<<<< HEAD
			v := value.ToJSON(value.List{value.Int(i * 20)})
=======
			v, _ := value.Marshal(value.NewList(value.Int(i * 20)))
>>>>>>> a76d697 (value: hide List/Dict behind struct vs naked typedef; disallow nested lists)
			p = append(p, profile.ProfileItemSer{OType: "user", Oid: 0, Key: "age", Version: i * 20, Value: v})
		}
		assert.NoError(t, c.setBatch(ctx, tier, p))
	}()
	go func() {
		p := make([]profile.ProfileItemSer, 0)
		for i := uint64(3); i >= 1; i-- {
			defer wg.Done()
<<<<<<< HEAD
			v := value.ToJSON(value.List{value.Int(i * 20)})
=======
			v, _ := value.Marshal(value.NewList(value.Int(i * 20)))
>>>>>>> a76d697 (value: hide List/Dict behind struct vs naked typedef; disallow nested lists)
			p = append(p, profile.ProfileItemSer{OType: "user", Oid: 1, Key: "age", Version: i * 20, Value: v})
		}
		assert.NoError(t, c.setBatch(ctx, tier, p))
	}()
	wg.Wait()

	// check that the latest profile can be accessed by provided version = 0
	// these should return values set as part of the second go routine above
	v, err := tier.Cache.Get(ctx, makeKey("user", 0, "age", 0))
	assert.NoError(t, err)
	// ("user", 0, "age", 60) would be the lastest profile
<<<<<<< HEAD
	expectedv := value.ToJSON(value.List{value.Int(60)})
=======
	expectedv, _ := value.Marshal(value.NewList(value.Int(60)))
>>>>>>> a76d697 (value: hide List/Dict behind struct vs naked typedef; disallow nested lists)
	assert.Equal(t, expectedv, []byte(v.(string)))

	v, err = tier.Cache.Get(ctx, makeKey("user", 1, "age", 0))
	assert.NoError(t, err)
	// ("user", 1, "age", 60) would be the lastest profile
<<<<<<< HEAD
	expectedv = value.ToJSON(value.List{value.Int(60)})
=======
	expectedv, _ = value.Marshal(value.NewList(value.Int(60)))
>>>>>>> a76d697 (value: hide List/Dict behind struct vs naked typedef; disallow nested lists)
	assert.Equal(t, expectedv, []byte(v.(string)))
}
