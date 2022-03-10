package profile

import (
	"context"
	"fmt"
	"time"

	"fennel/lib/cache"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/tier"

	"go.uber.org/zap"
)

// increment this to invalidate all existing cache keys for profile
const cacheVersion = 0

//================================================
// Public API for profile model (includes caching)
//================================================

func Set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	return cachedProvider{base: dbProvider{}}.set(ctx, tier, otype, oid, key, version, valueSer)
}

func Get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	return cachedProvider{base: dbProvider{}}.get(ctx, tier, otype, oid, key, version)
}

func GetBatched(ctx context.Context, tier tier.Tier, reqs []profile.ProfileItem) ([][]byte, error) {
	return cachedProvider{base: dbProvider{}}.getBatched(ctx, tier, reqs)
}

//================================================
// Private helpers/interface
//================================================

type cachedProvider struct {
	base provider
}

func (c cachedProvider) set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	defer timer.Start(ctx, tier.ID, "model.profile.cached.set").Stop()
	if err := c.base.set(ctx, tier, otype, oid, key, version, valueSer); err != nil {
		return err
	}
	// ground truth was successful so now we update the caches
	k1 := makeKey(otype, oid, key, version)
	err := tier.Cache.Set(ctx, k1, valueSer, 0 /* no expiry */)
	if err != nil {
		return err
	}

	// this could be the latest version for the profile, optimistically set it
	k2 := makeKey(otype, oid, key, 0)
	txnLogic := func(txn cache.Txn, ks []string) error {
		if len(ks) != 1 {
			return fmt.Errorf("expected only one key, given: %+v", ks)
		}
		v, err := c.getversion(ctx, tier, otype, oid, key)
		if err != nil {
			return err
		}

		// checking against the ground truth version helps minimize the conflict on concurrent sets.
		//
		// say two set commands with versions v0 < v1 concurrently reach this stage, `c.getversion`
		// returns the highest version for (otype, oid, key) - say v2. It is possible that v1 == v2,
		// in which case the value of v1 should be written. v2 > v1, in which case we wouldn't update
		// the cache.
		if v <= version {
			err = txn.Set(ctx, ks[0], valueSer, 0 /* no expiry */)
			if err != nil {
				return err
			}
		}
		return nil
	}
	// we retry atmost 3 times before we fail. Above logic should not fail on concurrent writes, but
	// it is possible that there could be a conflict with "get".
	//
	// say, `set` is called with version v1 > v0 (latest version) but the entry for the latest profile
	// (version = 0) was evicted. A concurrent `get` call could update the cache with the value corresponding
	// to v0 and the above logic aborting due to lack of retries.
	return tier.Cache.RunAsTxn(ctx, txnLogic, []string{k2}, 3)
}

func (c cachedProvider) getversion(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string) (uint64, error) {
	return c.base.getversion(ctx, tier, otype, oid, key)
}

func (c cachedProvider) get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	defer timer.Start(ctx, tier.ID, "model.profile.cached.get").Stop()
	ret, err := c.getBatched(ctx, tier, []profile.ProfileItem{{OType: otype, Oid: oid, Key: key, Version: version}})
	if err != nil {
		return nil, err
	}
	return ret[0], nil
}

func (c cachedProvider) getBatched(ctx context.Context, tier tier.Tier, reqs []profile.ProfileItem) ([][]byte, error) {
	defer timer.Start(ctx, tier.ID, "model.profile.cached.get_batched").Stop()
	rets := make([][]byte, len(reqs))
	keys := make([]string, len(reqs))
	keyToReq := make(map[string]profile.ProfileItem)
	keyToInd := make(map[string]int)
	for i, req := range reqs {
		keys[i] = makeKey(req.OType, req.Oid, req.Key, req.Version)
		keyToReq[keys[i]] = req
		keyToInd[keys[i]] = i
	}

	// run the logic as part of a txn
	//
	// NOTE: the logic here should assume that it could be retried if one of the provided keys
	// are updated during it's execution
	txnLogic := func(tx cache.Txn, ks []string) error {
		vals, err := tx.MGet(ctx, ks...)
		if err != nil {
			// if we got an error from cache, no need to panic - we just pretend nothing was found in cache
			for i := range vals {
				vals[i] = tier.Cache.Nil()
			}
		}
		tosetKeys := make([]string, 0)
		tosetVals := make([]interface{}, 0)

		for i, key := range ks {
			v := vals[i]
			if v == tier.Cache.Nil() {
				req := keyToReq[key]
				v, err = c.base.get(ctx, tier, req.OType, req.Oid, req.Key, req.Version)
				v2 := v.([]byte)
				// we only want to set in cache when ground truth has non-nil result
				// but v is an interface, so we first have to cast it in byte[] and then check
				// for nil
				if err == nil && v2 != nil {
					// if we could not find in cache but can find in ground truth, set in cache
					tosetKeys = append(tosetKeys, key)
					tosetVals = append(tosetVals, v)
				}
			}
			// since v is technically an interface, and we want to return []byte, we will take
			// attempts at converting tier to []byte
			switch t := v.(type) {
			case []byte:
				rets[keyToInd[key]] = t
			case string:
				rets[keyToInd[key]] = []byte(t)
			default:
				return fmt.Errorf("value not of type []byte or string: %v", v)
			}
		}
		return tx.MSet(ctx, tosetKeys, tosetVals, make([]time.Duration, len(tosetVals)))
	}
	// we retry this logic atmost 3 times after which we fail
	err := tier.Cache.RunAsTxn(ctx, txnLogic, keys, 3)

	// to avoid breaking critical workflows (computing profile aggregate values), silently
	// discard the error and return the profiles fetched as part of the execution of `txnLogic` above.
	//
	// NOTE: It is possible that the profiles for (>=1) of the keys are stale here. Consider:
	// on the last retry attempt of the `txnLogic`, profile corresponding to a key was updated in the Cache
	// concurrently, triggering another retry. This would abort the txn and invalidate the cache entries
	// for the keys; but it is possible that an entry was made in `rets[]` for the corresponding key
	// in one of the earlier attempts and was never overwritten in the later attempts.
	if err != nil {
		tier.Logger.Error("returning (potentially) partial results, txn failed with: ", zap.Error(err))
	}
	return rets, nil
}

var _ provider = cachedProvider{}

func cacheName() string {
	return "cache:profile"
}

func makeKey(otype ftypes.OType, oid uint64, key string, version uint64) string {
	prefix := fmt.Sprintf("%s:%d", cacheName(), cacheVersion)
	return fmt.Sprintf("%s:{%s:%d:%s}:%d", prefix, otype, oid, key, version)
}
