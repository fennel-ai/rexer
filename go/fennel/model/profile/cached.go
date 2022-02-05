package profile

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/tier"
	"fmt"
)

// increment this to invalidate all existing cache keys for profile
const cacheVersion = 0

//================================================
// Public API for profile model (includes caching)
//================================================

func Set(tier tier.Tier, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	return cachedProvider{base: dbProvider{}}.set(tier, custid, otype, oid, key, version, valueSer)
}

func Get(tier tier.Tier, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	return cachedProvider{base: dbProvider{}}.get(tier, custid, otype, oid, key, version)
}

//================================================
// Private helpers/interface
//================================================

type cachedProvider struct {
	base provider
}

func (c cachedProvider) set(tier tier.Tier, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	if err := c.base.set(tier, custid, otype, oid, key, version, valueSer); err != nil {
		return err
	}
	// ground truth was successful so now we update the caches
	k1 := makeKey(tier, custid, otype, oid, key, version)
	err1 := tier.Cache.Delete(context.TODO(), k1)

	// whenever we make a write, also invalidate "latest" version
	k2 := makeKey(tier, custid, otype, oid, key, 0)
	err2 := tier.Cache.Delete(context.TODO(), k2)
	var ret error = nil
	if err1 != nil && err1 != tier.Cache.Nil() {
		ret = err1
	}
	if err2 != nil && err2 != tier.Cache.Nil() {
		if ret != nil {
			ret = fmt.Errorf("%w; %v", err2, ret)
		} else {
			ret = err2
		}
	}
	return ret
}

func (c cachedProvider) get(tier tier.Tier, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	k := makeKey(tier, custid, otype, oid, key, version)
	v, err := tier.Cache.Get(context.TODO(), k)
	if err != nil {
		v, err = c.base.get(tier, custid, otype, oid, key, version)
		if err == nil {
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

func makeKey(tier tier.Tier, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64) string {
	prefix := fmt.Sprintf("%d:%s:%d", tier.ID, cacheName(), cacheVersion)
	return fmt.Sprintf("%s:%d:%s:%d:%s:%d", prefix, custid, otype, oid, key, version)
}
