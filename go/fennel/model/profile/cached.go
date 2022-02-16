package profile

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/tier"
	"fmt"
	"time"
)

// increment this to invalidate all existing cache keys for profile
const cacheVersion = 0

//================================================
// Public API for profile model (includes caching)
//================================================

func Set(tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	return cachedProvider{base: dbProvider{}}.set(tier, otype, oid, key, version, valueSer)
}

func Get(tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	return cachedProvider{base: dbProvider{}}.get(tier, otype, oid, key, version)
}

func GetBatched(tier tier.Tier, reqs []profile.ProfileItem) ([][]byte, error) {
	return cachedProvider{base: dbProvider{}}.getBatched(tier, reqs)
}

//================================================
// Private helpers/interface
//================================================

type cachedProvider struct {
	base provider
}

func (c cachedProvider) set(tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	defer timer.Start(tier.ID, "model.profile.cached.set").ObserveDuration()
	if err := c.base.set(tier, otype, oid, key, version, valueSer); err != nil {
		return err
	}
	// ground truth was successful so now we update the caches
	k1 := makeKey(otype, oid, key, version)
	// whenever we make a write, also invalidate "latest" version
	k2 := makeKey(otype, oid, key, 0)
	err := tier.Cache.Delete(context.TODO(), k1, k2)
	if err != tier.Cache.Nil() {
		return err
	}
	return nil
}

func (c cachedProvider) get(tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	defer timer.Start(tier.ID, "model.profile.cached.get").ObserveDuration()
	ret, err := c.getBatched(tier, []profile.ProfileItem{{OType: otype, Oid: oid, Key: key, Version: version}})
	if err != nil {
		return nil, err
	}
	return ret[0], nil
}

func (c cachedProvider) getBatched(tier tier.Tier, reqs []profile.ProfileItem) ([][]byte, error) {
	defer timer.Start(tier.ID, "model.profile.cached.get_batched").ObserveDuration()
	rets := make([][]byte, len(reqs))
	keys := make([]string, len(reqs))
	for i, req := range reqs {
		keys[i] = makeKey(req.OType, req.Oid, req.Key, req.Version)
	}
	vals, err := tier.Cache.MGet(context.TODO(), keys...)
	if err != nil {
		// if we got an error from cache, no need to panic - we just pretend nothing was found in cache
		for i := range vals {
			vals[i] = tier.Cache.Nil()
		}
	}
	tosetKeys := make([]string, 0)
	tosetVals := make([]interface{}, 0)

	for i, v := range vals {
		if v == tier.Cache.Nil() {
			req := reqs[i]
			v, err = c.base.get(tier, req.OType, req.Oid, req.Key, req.Version)
			v2 := v.([]byte)
			// we only want to set in cache when ground truth has non-nil result
			// but v is an interface, so we first have to cast it in byte[] and then check
			// for nil
			if err == nil && v2 != nil {
				// if we could not find in cache but can find in ground truth, set in cache
				tosetKeys = append(tosetKeys, keys[i])
				tosetVals = append(tosetVals, v)
			}
		}
		// since v is technically an interface, and we want to return []byte, we will take
		// attempts at converting tier to []byte
		switch t := v.(type) {
		case []byte:
			rets[i] = t
		case string:
			rets[i] = []byte(t)
		default:
			return nil, fmt.Errorf("value not of type []byte or string: %v", v)
		}
	}
	err = tier.Cache.MSet(context.TODO(), tosetKeys, tosetVals, make([]time.Duration, len(tosetVals)))
	return rets, err
}

var _ provider = cachedProvider{}

func cacheName() string {
	return "cache:profile"
}

func makeKey(otype ftypes.OType, oid uint64, key string, version uint64) string {
	prefix := fmt.Sprintf("%s:%d", cacheName(), cacheVersion)
	return fmt.Sprintf("%s:%s:%d:%s:%d", prefix, otype, oid, key, version)
}
