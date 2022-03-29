package profile

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"fennel/lib/cache"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/tier"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// increment this to invalidate all existing cache keys for profile
const cacheVersion = 0

var profiles_cache_failures = promauto.NewCounter(prometheus.CounterOpts{
	Name: "profile_cache_mset_failures",
	Help: "Number of failures while trying to set versioned profiles in the cache",
})

//================================================
// Public API for profile model (includes caching)
//================================================

func Set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	return cachedProvider{base: dbProvider{}}.set(ctx, tier, otype, oid, key, version, valueSer)
}

func SetBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItemSer) error {
	return cachedProvider{base: dbProvider{}}.setBatch(ctx, tier, profiles)
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
	return c.setBatch(ctx, tier, []profile.ProfileItemSer{profile.NewProfileItemSer(string(otype), oid, key, version, valueSer)})
}

func (c cachedProvider) setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItemSer) error {
	defer timer.Start(ctx, tier.ID, "model.profile.cached.setBatch").Stop()

	// NOTE: the implementation assumes that in scenarios where the cache could be inconsistent with the DB, the caller would retry
	// which should lead to eventually consistency of the cache
	//
	// e.g. in case a process crash, say after setting profiles on the DB, the cache for the latest profiles is now potentially
	// inconsistent. Since the writes to the DB are idempotent, multiple retries for the same profile(s) will make the cache consistent
	// with the DB, if not with the profiles involved in the current, a concurrent call could succeed in setting the latest profile

	// Write to DB
	if err := c.base.setBatch(ctx, tier, profiles); err != nil {
		return err
	}

	// ground truth was successful so now we update the caches for each version
	keys := make([]string, len(profiles))
	valsToSet := make([]interface{}, len(profiles))
	for i, p := range profiles {
		k := makeKey(p.OType, p.Oid, p.Key, p.Version)
		keys[i] = k
		valsToSet[i] = p.Value
	}

	// If cache could not be updated for the versioned profiles, it is fine since on the next read calls,
	// the profile will be fetched from the DB and set on cache
	if err := tier.Cache.MSet(ctx, keys, valsToSet, make([]time.Duration, len(keys))); err != nil {
		tier.Logger.Warn("failed to set versioned profiles on cache. err: ", zap.Error(err))
		profiles_cache_failures.Inc()
	}

	// To store the latest, only consider the value of the largest version of the profile key (otype, oid, key)
	latestValByKey := make(map[versionIdentifier]profile.ProfileItemSer)

	for _, p := range profiles {
		verId := versionIdentifier{p.OType, p.Oid, p.Key}
		val, ok := latestValByKey[verId]
		if !ok {
			latestValByKey[verId] = p
		} else {
			if p.Version > val.Version {
				latestValByKey[verId] = p
			}
		}
	}

	latestKeys := make([]string, 0)
	for _, profile := range profiles {
		latestKeys = append(latestKeys, makeKey(profile.OType, profile.Oid, profile.Key, 0))
	}

	// few of the profiles could be the latest profiles, optimistically set them
	txnLogic := func(txn cache.Txn, ks []string) error {
		// get the latest version of a profile identified using (otype, oid, key) and compare it
		// with the latest versioned profile in the current batch, to figure out if the
		// cache entry for the "latest" profile should be updated
		vids := make([]versionIdentifier, 0)
		for _, k := range ks {
			vid, err := parseKey(k)
			if err != nil {
				return err
			}
			vids = append(vids, vid)
		}

		// if fetching versions failed, return the error. If this is not resolved after retries,
		// cache for the latest profiles will be invalidated which leaves the cache in a consistent state
		vMap, err := c.base.getVersionBatched(ctx, tier, vids)
		if err != nil {
			return err
		}

		keysToSet := make([]string, 0)
		valsToSet := make([]interface{}, 0)

		for _, k := range ks {
			vid, err := parseKey(k)
			if err != nil {
				return err
			}
			latestV, ok := vMap[vid]
			if !ok {
				// no version was found. Ideally this should never happen since DB should have
				// the data - we set the data in the same function.
				return fmt.Errorf("could not get version for profile: %s from the DB", k)
			}
			if p, ok := latestValByKey[vid]; ok {
				// checking against the ground truth version helps minimize the conflict on concurrent sets.
				//
				// say two set commands with versions v0 < v1 concurrently reach this stage, `c.getversion`
				// returns the highest version for (otype, oid, key) - say v2. It is possible that v1 == v2,
				// in which case the value of v1 should be written. v2 > v1, in which case we wouldn't update
				// the cache.
				if latestV <= p.Version {
					// set the value for the key in the cache!
					keysToSet = append(keysToSet, k)
					valsToSet = append(valsToSet, p.Value)
				}
			}
		}

		// Set them on cache
		return txn.MSet(ctx, keysToSet, valsToSet, make([]time.Duration, len(keysToSet)))
	}
	// we retry atmost 3 times before we give up. Above logic should not fail on concurrent writes, but
	// it is possible that there could be a conflict with "get".
	//
	// say, `set` is called with version v1 > v0 (latest version) but the entry for the latest profile
	// (version = 0) was evicted. A concurrent `get` call could update the cache with the value corresponding
	// to v0 and the above logic aborting due to lack of retries.
	return tier.Cache.RunAsTxn(ctx, txnLogic, latestKeys, 3)
}

func (c cachedProvider) getVersionBatched(ctx context.Context, tier tier.Tier, vids []versionIdentifier) (map[versionIdentifier]uint64, error) {
	return c.base.getVersionBatched(ctx, tier, vids)
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

	// Dedup keys to avoid I/O from cache and DB.
	keyMap := make(map[string]struct{})
	keyToReq := make(map[string]profile.ProfileItem)
	for _, req := range reqs {
		key := makeKey(req.OType, req.Oid, req.Key, req.Version)
		keyMap[key] = struct{}{}
		keyToReq[key] = req
	}

	keys := make([]string, 0)
	for k := range keyMap {
		keys = append(keys, k)
	}

	keyToVal := make(map[string][]byte)
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
				keyToVal[key] = t
			case string:
				keyToVal[key] = []byte(t)
			default:
				return fmt.Errorf("value not of type []byte or string: %v", v)
			}
		}
		return tx.MSet(ctx, tosetKeys, tosetVals, make([]time.Duration, len(tosetVals)))
	}
	// we retry this logic atmost 3 times after which we fail
	//
	// to avoid breaking critical workflows (computing profile aggregate values), silently
	// discard the error and return the profiles fetched as part of the execution of `txnLogic` above.
	//
	// NOTE: It is possible that the profiles for (>=1) of the keys are stale here. Consider:
	// on the last retry attempt of the `txnLogic`, profile corresponding to a key was updated in the Cache
	// concurrently, triggering another retry. This would abort the txn and invalidate the cache entries
	// for the keys; but it is possible that an entry was made in `rets[]` for the corresponding key
	// in one of the earlier attempts and was never overwritten in the later attempts.
	if err := tier.Cache.RunAsTxn(ctx, txnLogic, keys, 3); err != nil {
		tier.Logger.Error("returning (potentially) partial results, txn failed with: ", zap.Error(err))
	}

	// Set the return values from the values we have fetched so far
	rets := make([][]byte, len(reqs))
	for i, req := range reqs {
		key := makeKey(req.OType, req.Oid, req.Key, req.Version)
		if v, ok := keyToVal[key]; ok {
			rets[i] = v
		} else {
			// Return nil to show that either the profile does not exist or could not be fetched
			rets[i] = nil
		}
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

func parseKey(key string) (versionIdentifier, error) {
	re, err := regexp.Compile(":{(.+):(\\d+):(.+)}:")
	if err != nil {
		return versionIdentifier{}, err
	}
	match := re.FindStringSubmatch(key)
	if match != nil {
		// match[0] is the string matched
		vid := versionIdentifier{otype: ftypes.OType(match[1]), key: match[3]}
		vid.oid, err = strconv.ParseUint(match[2], 10, 64)
		if err != nil {
			return versionIdentifier{}, err
		}
		return vid, nil
	}
	return versionIdentifier{}, fmt.Errorf("failed to parse key: %s", key)
}
