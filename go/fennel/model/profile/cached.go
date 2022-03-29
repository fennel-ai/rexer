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

	"go.uber.org/zap"
)

// increment this to invalidate all existing cache keys for profile
const cacheVersion = 0

// lease duration in seconds to set on latest profiles in the cache
const leaseDuration = 10

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
	// watch on both the versioned and latest profiles
	//
	// latest profiles are required to be sequentially updated by concurrent updates for the same "keyed-profiles". Also
	// in case of cache eviction, concurrent `set` and `get` for the latest profile could lead into cache being in inconsistent state
	//
	// since `RunAsTxn` runs in a sharded way, versioned profiles are also being watched so that in the txn logic knows the
	// sharded keys to work with
	keys := make([]string, 0)
	profileByKey := make(map[string]profile.ProfileItemSer)
	for _, profile := range profiles {
		if err := profile.Validate(); err != nil {
			return err
		}
		k := makeKey(profile.OType, profile.Oid, profile.Key, profile.Version)
		latestk := makeKey(profile.OType, profile.Oid, profile.Key, 0)
		keys = append(keys, k)
		keys = append(keys, latestk)
		profileByKey[k] = profile
	}

	txnLogic := func(txn cache.Txn, ks []string) error {
		// separate out keys corresponding to versioned profiles and latest profiles
		latestKeys := make([]string, 0)
		versionedKeys := make([]string, 0)
		for _, k := range ks {
			ok, err := isLatestProfile(k)
			if err != nil {
				return nil
			}
			if ok {
				latestKeys = append(latestKeys, k)
			} else {
				versionedKeys = append(versionedKeys, k)
			}
		}

		// set a lease on the latest profiles
		// this is to avoid any cache inconsistencies which could arise due to a successful write to the DB, but failure
		// to update the latest profiles in the cache (e.g. process crash)
		lease := make([]time.Duration, 0)
		for i := 0; i < len(latestKeys); i++ {
			lease = append(lease, time.Duration(leaseDuration)*time.Second)
		}
		if err := txn.Expire(ctx, latestKeys, lease); err != nil {
			tier.Logger.Error("failed to set lease on latest keys with: ", zap.Error(err))
			return err
		}

		profilesToWrite := make([]profile.ProfileItemSer, 0)
		keysToSet := make([]string, 0)
		valuesToSet := make([]interface{}, 0)
		for _, k := range versionedKeys {
			p, ok := profileByKey[k]
			if !ok {
				tier.Logger.Error(fmt.Sprintf("profile value not present for key : %+v", k))
				return fmt.Errorf("profile value not present for key: %+v", k)
			}
			keysToSet = append(keysToSet, k)
			valuesToSet = append(valuesToSet, p.Value)
			profilesToWrite = append(profilesToWrite, p)
		}

		// write to DB
		if err := c.base.setBatch(ctx, tier, profilesToWrite); err != nil {
			tier.Logger.Error("writing to DB failed with: ", zap.Error(err))
			return err
		}

		// if updating the cache with the versioned profiles fail, do not fail the request as the
		// latest profiles are invalidated due to the lease set on them
		if err := txn.MSetNoTxn(ctx, keysToSet, valuesToSet, make([]time.Duration, len(keysToSet))); err != nil {
			tier.Logger.Warn("cache MSet for versioned profiles failed with: ", zap.Error(err))
			return err
		}

		// only consider the profiles with the largest versions in the current batch
		// To store the latest, only consider the value of the largest version of the profile (otype, oid, key)
		latestValByKey := make(map[versionIdentifier]profile.ProfileItemSer)

		vids := make([]versionIdentifier, 0)
		for _, k := range versionedKeys {
			verId, err := parseKey(k)
			if err != nil {
				return err
			}
			vids = append(vids, verId)
			p, ok := profileByKey[k]
			if !ok {
				tier.Logger.Error(fmt.Sprintf("profile not found for key : %+v", k))
				return fmt.Errorf("profile not found for key: +%v", k)
			}
			val, ok := latestValByKey[verId]
			if !ok {
				latestValByKey[verId] = p
			} else {
				if p.Version > val.Version {
					latestValByKey[verId] = p
				}
			}
		}

		vMap, err := c.base.getVersionBatched(ctx, tier, vids)
		if err != nil {
			return err
		}

		latestKeysToUpdate := make([]string, 0)
		latestValuesToUpdate := make([]interface{}, 0)
		// write the latest profiles optimistically
		for _, k := range latestKeys {
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
			p, ok := latestValByKey[vid]
			if !ok {
				return fmt.Errorf("latest value not found for key: +%v", vid)
			}
			// checking against the ground truth version helps minimize the conflict on concurrent sets.
			//
			// say two set commands with versions v0 < v1 concurrently reach this stage, `c.getversion`
			// returns the highest version for (otype, oid, key) - say v2. It is possible that v1 == v2,
			// in which case the value of v1 should be written. v2 > v1, in which case we wouldn't update
			// the cache.
			if latestV <= p.Version {
				// set the value for the key in the cache!
				latestKeysToUpdate = append(latestKeysToUpdate, k)
				latestValuesToUpdate = append(latestValuesToUpdate, p.Value)
			}
		}
		// set them on cache
		err = txn.MSetNoTxn(ctx, latestKeysToUpdate, latestValuesToUpdate, make([]time.Duration, len(latestKeysToUpdate)))
		if err != nil {
			tier.Logger.Warn("Failed to set latest profiles on cache with: ", zap.Error(err))
			return err
		}
		// persist the keys for the latest profile which were not modified during the lifetime of the txn
		if err := txn.Persist(ctx, latestKeys); err != nil {
			// if extension failed, it is okay since the next read for this profile will fetch the latest version
			// and update the cache as well
			tier.Logger.Warn("Failed to extend the TTL with: ", zap.Error(err))
		}
		return nil
	}
	// we retry atmost 3 times before we fail. Above logic protects from different concurrent writes. E.g.
	//
	// say, `set` is called with version v1 > v0 (latest version) but the entry for the latest profile
	// (version = 0) was evicted. A concurrent `get` call could update the cache with the value corresponding
	// to v0 and the above logic aborting due to lack of retries.
	return tier.Cache.RunAsTxn(ctx, txnLogic, keys, 3)
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
		return tx.MSetNoTxn(ctx, tosetKeys, tosetVals, make([]time.Duration, len(tosetVals)))
	}
	// we retry this logic atmost 3 times after which we fail
	err := tier.Cache.RunAsTxn(ctx, txnLogic, keys, 3 /*r=*/)

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

func isLatestProfile(k string) (bool, error) {
	re, err := regexp.Compile(":{(.+):(\\d+):(.+)}:(\\d+)")
	if err != nil {
		return false, err
	}
	match := re.FindStringSubmatch(k)
	if match != nil {
		if len(match) != 5 {
			return false, fmt.Errorf("failed to parse key: %s, should have at least 5 matchers, has: %d", k, len(match))
		}
		v, err := strconv.ParseUint(match[4], 10, 64)
		if err != nil {
			return false, err
		}
		return v == 0, nil
	}
	return false, fmt.Errorf("failed to parse key: %s", k)
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
