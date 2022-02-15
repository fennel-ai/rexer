package profile

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/tier"
	"fmt"
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
	k := makeKey(otype, oid, key, version)
	v, err := tier.Cache.Get(context.TODO(), k)
	if err == tier.Cache.Nil() {
		v, err = c.base.get(tier, otype, oid, key, version)
		v2 := v.([]byte)
		// we only want to set in cache when ground truth has non-nil result
		// but v is an interface, so we first have to cast it in byte[] and then check
		// for nil
		if err == nil && v2 != nil {
			// if we could not find in cache but can find in ground truth, set in cache
			err = tier.Cache.Set(context.TODO(), k, v, 0)
		}
	}

	// since v is technically an interface, and we want to return []byte, we will take
	// attempts at converting tier to []byte
	if v_ret, ok := v.([]byte); ok {
		return v_ret, err
	}
	if v_ret, ok := v.(string); ok {
		return []byte(v_ret), err
	}
	return nil, fmt.Errorf("value not of type []byte or string: %v", v)
}

var _ provider = cachedProvider{}

func cacheName() string {
	return "cache:profile"
}

func makeKey(otype ftypes.OType, oid uint64, key string, version uint64) string {
	prefix := fmt.Sprintf("%s:%d", cacheName(), cacheVersion)
	return fmt.Sprintf("%s:%s:%d:%s:%d", prefix, otype, oid, key, version)
}
