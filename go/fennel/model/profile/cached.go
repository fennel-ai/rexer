package profile

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fennel/lib/cache"
	"fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/lib/value"
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

func Set(ctx context.Context, tier tier.Tier, profile profile.ProfileItem) error {
	return cachedProvider{base: dbProvider{}}.set(ctx, tier, profile)
}

func SetBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItem) error {
	return cachedProvider{base: dbProvider{}}.setBatch(ctx, tier, profiles)
}

func Get(ctx context.Context, tier tier.Tier, profileKey profile.ProfileItemKey) (profile.ProfileItem, error) {
	return cachedProvider{base: dbProvider{}}.get(ctx, tier, profileKey)
}

func GetBatch(ctx context.Context, tier tier.Tier, profileKeys []profile.ProfileItemKey) ([]profile.ProfileItem, error) {
	return cachedProvider{base: dbProvider{}}.getBatch(ctx, tier, profileKeys)
}

//================================================
// Private helpers/interface
//================================================

type cachedProvider struct {
	base provider
}

func (c cachedProvider) set(ctx context.Context, tier tier.Tier, profileItem profile.ProfileItem) error {
	defer timer.Start(ctx, tier.ID, "model.profile.cached.set").Stop()
	return c.setBatch(ctx, tier, []profile.ProfileItem{profileItem})
}

func (c cachedProvider) setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItem) error {
	defer timer.Start(ctx, tier.ID, "model.profile.cached.setBatch").Stop()

	// NOTE: the implementation assumes that in scenarios where the cache could be inconsistent with the DB, the caller would retry
	// which should lead to eventually consistency of the cache
	//
	// e.g. in case a process crash, say after setting profiles on the DB, the cache for the latest profiles is now potentially
	// inconsistent. Since the writes to the DB are idempotent, multiple retries for the same profile(s) will make the cache consistent
	// with the DB, if not with the profiles involved in the current, a concurrent call could succeed in setting the latest profile

	// Dedup the profiles to avoid setting the same profile twice.
	// To store the latest, only consider the value of the largest version of the profile key (otype, oid, key)
	latestProfileByKey := make(map[profile.ProfileItemKey]profile.ProfileItem)

	for _, p := range profiles {
		pk := profile.NewProfileItemKey(string(p.OType), p.Oid, p.Key)
		val, ok := latestProfileByKey[pk]
		if !ok {
			latestProfileByKey[pk] = p
		} else {
			if p.UpdateTime > val.UpdateTime {
				latestProfileByKey[pk] = p
			}
		}
	}

	num_unique_profiles := len(latestProfileByKey)
	latestProfiles := make([]profile.ProfileItem, 0, num_unique_profiles)
	latestKeys := make([]string, 0)

	keyToProfileKey := make(map[string]profile.ProfileItemKey)
	for pk, profile := range latestProfileByKey {
		latestProfiles = append(latestProfiles, profile)
		key := makeKey(pk)
		latestKeys = append(latestKeys, key)
		keyToProfileKey[key] = pk
	}

	// Write to DB
	if err := c.base.setBatch(ctx, tier, latestProfiles); err != nil {
		return err
	}

	txnLogic := func(txn cache.Txn, ks []string) error {
		profileKeys := make([]profile.ProfileItemKey, 0)
		for _, k := range ks {
			profileKeys = append(profileKeys, keyToProfileKey[k])
		}

		// if fetching versions failed, return the error. If this is not resolved after retries,
		// cache for the latest profiles will be invalidated which leaves the cache in a consistent state
		profiles, err := c.base.getBatch(ctx, tier, profileKeys)
		if err != nil {
			fmt.Println("Raised here", err)
			return err
		}
		tosetKeys := make([]string, 0)
		tosetVals := make([]interface{}, 0)
		for i, profileItem := range profiles {
			if profileItem.Value == value.Nil {
				tier.Logger.Error("Found nil value in setBatch for profile", zap.String("key", profileItem.Key), zap.Uint64("profile_id", profileItem.Oid))
				continue
			}
			key := ks[i]
			tosetKeys = append(tosetKeys, key)
			tosetVals = append(tosetVals, value.ToJSON(profileItem.Value))
		}
		// Set them on cache
		return txn.MSet(ctx, tosetKeys, tosetVals, make([]time.Duration, len(latestKeys)))
	}
	// we retry atmost 3 times before we give up. Above logic should not fail on concurrent writes, but
	// it is possible that there could be a conflict with "get".
	//
	// say, `set` is called with version v1 > v0 (latest version) but the entry for the latest profile
	// (version = 0) was evicted. A concurrent `get` call could update the cache with the value corresponding
	// to v0 and the above logic aborting due to lack of retries.
	return tier.Cache.RunAsTxn(ctx, txnLogic, latestKeys, 3)
}

func (c cachedProvider) get(ctx context.Context, tier tier.Tier, profileKey profile.ProfileItemKey) (profile.ProfileItem, error) {
	defer timer.Start(ctx, tier.ID, "model.profile.cached.get").Stop()
	ret, err := c.getBatch(ctx, tier, []profile.ProfileItemKey{profileKey})
	if err != nil || len(ret) == 0 {
		return profile.NewProfileItem(string(profileKey.OType), profileKey.Oid, profileKey.Key, value.Nil, 0), err
	}

	return ret[0], nil
}

func getValueFromCache(v interface{}) (value.Value, error) {
	switch t := v.(type) {
	case []byte:
		return value.FromJSON(t)
	case string:
		return value.FromJSON([]byte(t))
	default:
		return nil, fmt.Errorf("value not of type []byte or string: %v", v)
	}
}
func (c cachedProvider) getBatch(ctx context.Context, tier tier.Tier, profileKeys []profile.ProfileItemKey) ([]profile.ProfileItem, error) {
	defer timer.Start(ctx, tier.ID, "model.profile.cached.get_batched").Stop()

	// Dedup keys to avoid I/O from cache and DB.
	keyToProfileKey := make(map[string]profile.ProfileItemKey)
	allKeys := make([]string, 0)
	for _, pk := range profileKeys {
		key := makeKey(pk)
		allKeys = append(allKeys, key)
		keyToProfileKey[key] = pk
	}

	keys := make([]string, 0)
	for k, _ := range keyToProfileKey {
		keys = append(keys, k)

	}


	vals, err := tier.Cache.MGet(ctx, keys...)
	unavailableKeys := make([]string, 0)

	// profile key to profile map
	var keyToVal sync.Map
	rets := make([]profile.ProfileItem, 0, len(profileKeys))
	if err != nil {
		// if we got an error from cache, no need to panic - we just pretend nothing was found in cache
		if len(vals) == 0 {
			vals = make([]interface{}, len(keys))
		}
		for i := range vals {
			vals[i] = tier.Cache.Nil()
			unavailableKeys = keys
		}
	} else {
		for i, v := range vals {
			if v == tier.Cache.Nil() {
				unavailableKeys = append(unavailableKeys, keys[i])
			} else {
				vc, err := getValueFromCache(v)
				if err == nil {
					pk := keyToProfileKey[keys[i]]
					profile := profile.NewProfileItem(string(pk.OType), pk.Oid, pk.Key, value.Nil, 0)
					profile.Value = vc.(value.Value)
					keyToVal.Store(keyToProfileKey[keys[i]], profile)
				}
			}
		}

		for i, pk := range profileKeys {
			if p, ok := keyToVal.Load(profileKeys[i]); !ok {
				rets = append(rets, profile.NewProfileItem(string(pk.OType), pk.Oid, pk.Key, value.Nil, 0))
			} else {
				rets = append(rets, p.(profile.ProfileItem))
			}
		}
	}
	// Could read from cache, return.
	if len(unavailableKeys) == 0 {
		return rets, nil
	}

	// run the logic as part of a txn
	//
	// NOTE: the logic here should assume that it could be retried if one of the provided keys
	// are updated during it's execution
	txnLogic := func(tx cache.Txn, ks []string) error {
		profileBatchRequest := make([]profile.ProfileItemKey, 0)
		for _, key := range ks {
			profileBatchRequest = append(profileBatchRequest, keyToProfileKey[key])
		vals, err := tx.MGet(ctx, ks...)
		if err != nil {
			// It is possible that upon getting the error (e.g. context cancelled), vals == nil or the length
			// does not match `ks`
			if vals == nil || len(vals) != len(ks) {
				vals = make([]interface{}, len(ks))
			}
			// if we got an error from cache, no need to panic - we just pretend nothing was found in cache
			for i := range ks {
				vals[i] = tier.Cache.Nil()
			}


		}

		tosetKeys := make([]string, 0)
		tosetVals := make([]interface{}, 0)
		dbProfiles, err := c.base.getBatch(ctx, tier, profileBatchRequest)

		if err != nil {
			return err
		}

		for _, profileItem := range dbProfiles {
			key := makeKey(profileItem.GetProfileKey())
			tosetKeys = append(tosetKeys, key)
			tosetVals = append(tosetVals, value.ToJSON(profileItem.Value))
			keyToVal.Store(profileItem.GetProfileKey(), profileItem)
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
	if err := tier.Cache.RunAsTxn(ctx, txnLogic, unavailableKeys, 3); err != nil {
		tier.Logger.Error("returning (potentially) partial results, txn failed with: ", zap.Error(err))
	}

	// Set the remaining unavailble keys to the profiles returned from DB and return.
	for i := 0; i < len(profileKeys); i++ {
		// If it was a cache miss, check from the DB call.
		if rets[i].Value == value.Nil {
			if p, ok := keyToVal.Load(profileKeys[i]); ok {
				rets[i] = p.(profile.ProfileItem)
			}
		}
	}
	return rets, nil
}

var _ provider = cachedProvider{}

func cacheName() string {
	return "cache:profile"
}

func makeKey(pk profile.ProfileItemKey) string {
	prefix := fmt.Sprintf("%s:%d", cacheName(), cacheVersion)
	return fmt.Sprintf("%s:{%s:%d:%s}", prefix, pk.OType, pk.Oid, pk.Key)
}
