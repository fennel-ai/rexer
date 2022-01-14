package data

import (
	"context"
	"fennel/cache"
	"fennel/profile/lib"
	"fmt"
)

// increment this to invalidate all existing cache keys for profile
const cacheVersion = 0

type CachedDB struct {
	groundTruth Provider
	cache       cache.Cache
}

var _ Provider = CachedDB{DB{"", nil}, nil}

func (c CachedDB) Init() error {
	if err := c.groundTruth.Init(); err != nil {
		return err
	}
	if err := c.cache.Init(); err != nil {
		return err
	}
	return nil
}

func (c CachedDB) Name() string {
	return fmt.Sprintf("cache:%s", c.groundTruth.Name())
}

func (c CachedDB) Set(otype lib.OType, oid lib.OidType, key string, version uint64, valueSer []byte) error {
	if err := c.groundTruth.Set(otype, oid, key, version, valueSer); err != nil {
		return err
	}
	// ground truth was successful so now we update the caches
	k1 := c.key(otype, oid, key, version)
	err1 := c.cache.Delete(context.TODO(), k1)

	// whenever we make a write, also invalidate "latest" version
	k2 := c.key(otype, oid, key, 0)
	err2 := c.cache.Delete(context.TODO(), k2)
	var ret error = nil
	if err1 != nil && err1 != c.cache.Nil() {
		ret = err1
	}
	if err2 != nil && err2 != c.cache.Nil() {
		if ret != nil {
			ret = fmt.Errorf("%w; %v", err2, ret)
		} else {
			ret = err2
		}
	}
	return ret
}

func (c CachedDB) Get(otype lib.OType, oid lib.OidType, key string, version uint64) ([]byte, error) {
	k := c.key(otype, oid, key, version)

	v, err := c.cache.Get(context.TODO(), k)
	if err != nil {
		v, err = c.groundTruth.Get(otype, oid, key, version)
		if err == nil {
			// if we could not find in cache but can find in ground truth, set in cache
			err = c.cache.Set(context.TODO(), k, v, 0)
		}
	}

	// since v is technically an interface, and we want to return []byte, we will take
	// attempts at converting this to []byte
	if v_ret, ok := v.([]byte); ok {
		return v_ret, err
	}
	if v_ret, ok := v.(string); ok {
		return []byte(v_ret), err
	}
	return nil, fmt.Errorf("value not of type []byte or string: %v", v)
}

func (c CachedDB) key(otype lib.OType, oid lib.OidType, key string, version uint64) string {
	prefix := fmt.Sprintf("%s:%d", c.Name(), cacheVersion)
	return fmt.Sprintf("%s:%d:%d:%s:%d", prefix, otype, oid, key, version)
}
