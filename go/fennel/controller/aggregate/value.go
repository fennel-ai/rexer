package aggregate

import (
	"context"
	"fmt"
	"os"
	"time"

	"fennel/controller/counter"
	"fennel/engine"
	"fennel/engine/ast"
	"fennel/engine/interpreter/bootarg"
	"fennel/lib/action"
	"fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/phaser"
	"fennel/lib/profile"
	"fennel/lib/value"
	modelCounter "fennel/model/counter"
	"fennel/tier"

	"go.uber.org/zap"
)

const cacheValueDuration = 2 * time.Minute

// increment this to invalidate all existing cache keys for aggregate
var cacheVersion = 0

func InvalidateCache() {
	cacheVersion++
}

func Value(
	ctx context.Context, tier tier.Tier, name ftypes.AggName, key value.Value, kwargs value.Dict,
) (value.Value, error) {
	if disableCache, present := os.LookupEnv("DISABLE_CACHE"); present && disableCache == "1" {
		return unitValue(ctx, tier, name, key, kwargs)
	}

	ckey := makeCacheKey(name, key, kwargs)
	// If already present in cache and no failure interpreting it, return directly
	if v, ok := tier.PCache.Get(ckey, "AggValue"); ok {
		if val, ok2 := fromCacheValue(tier, v); ok2 {
			return val, nil
		}
	}
	// otherwise compute value and store in cache
	val, err := unitValue(ctx, tier, name, key, kwargs)
	if err != nil {
		return nil, err
	}

	if !tier.PCache.SetWithTTL(ckey, val, int64(len(ckey)+len(val.String())), cacheValueDuration, "AggValue") {
		tier.Logger.Debug(fmt.Sprintf("failed to set aggregate value in cache: key: '%s' value: '%s'", ckey, val.String()))
	}
	return val, nil
}

func BatchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	if disableCache, present := os.LookupEnv("DISABLE_CACHE"); present && disableCache == "1" {
		return batchValue(ctx, tier, batch)
	}

	ret := make([]value.Value, len(batch))
	uckeys := make([]string, len(batch))
	uncachedReqs := make([]aggregate.GetAggValueRequest, len(batch))
	ptr := make([]int, len(batch))
	seen := make(map[string]int, len(batch))

	j := 0
	for i, req := range batch {
		ckey := makeCacheKey(req.AggName, req.Key, req.Kwargs)
		if v, ok := tier.PCache.Get(ckey, "AggValue"); ok {
			if val, found := fromCacheValue(tier, v); found {
				ret[i] = val
				ptr[i] = -1
			}
		}
		// check if we could get it from cache or not
		if ptr[i] != -1 {
			// not in cache, so we may do a ground truth pull
			// but first check if this is a duplicate request
			if idx, repeat := seen[ckey]; repeat {
				ptr[i] = idx // duplicate, so use the same index
			} else {
				// if not duplicate, add to uncached requests
				seen[ckey] = j
				uckeys[j] = ckey
				uncachedReqs[j] = req
				ptr[i] = j
				j += 1
			}
		}
	}
	ucvals, err := batchValue(ctx, tier, uncachedReqs[:j])
	if err != nil {
		return nil, err
	}
	for i := range batch {
		idx := ptr[i]
		if idx < 0 {
			continue
		}
		ret[i] = ucvals[idx]
		if ok := tier.PCache.SetWithTTL(uckeys[idx], ucvals[idx], int64(len(uckeys[idx])+len(ucvals[idx].String())), cacheValueDuration, "AggValue"); !ok {
			tier.Logger.Debug(fmt.Sprintf("failed to set aggregate value in cache: key: '%s' value: '%s'", uckeys[idx], ucvals[idx].String()))
		}
	}
	return ret, nil
}

func unitValue(
	ctx context.Context, tier tier.Tier, name ftypes.AggName, key value.Value, kwargs value.Dict,
) (value.Value, error) {
	agg, err := Retrieve(ctx, tier, name)
	if err != nil {
		return value.Nil, err
	}
	histogram, err := modelCounter.ToHistogram(agg.Id, agg.Options)
	if err != nil {
		return nil, err
	}
	return counter.Value(ctx, tier, agg.Id, key, histogram, kwargs)
}

func getDuration(kwargs value.Dict) (int, error) {
	d, ok := kwargs.Get("duration")
	if !ok {
		return 0, fmt.Errorf("error: no duration specified")
	}
	duration, ok := d.(value.Int)
	if !ok {
		return 0, fmt.Errorf("error: expected kwarg 'duration' to be an int but found: '%v'", d)
	}
	return int(duration), nil
}

func batchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	n := len(batch)

	unique := make(map[ftypes.AggName]aggregate.Aggregate)
	for _, req := range batch {
		unique[req.AggName] = aggregate.Aggregate{}
	}
	var err error
	for name := range unique {
		unique[name], err = Retrieve(ctx, tier, name)
		if err != nil {
			tier.Logger.Error("failed to retrieve aggregate", zap.String("aggregate: ", string(name)), zap.Error(fmt.Errorf("%w", err)))
			return nil, fmt.Errorf("failed to retrieve aggregate %s", name)
		}
	}

	ret := make([]value.Value, n)
	numSlotsLeft, err := fetchOfflineAggregates(tier, unique, batch, ret)
	if err != nil {
		return nil, err
	}

	numSlotsLeft, err = fetchForeverAggregates(ctx, tier, unique, batch, ret, numSlotsLeft)
	if err != nil {
		return nil, err
	}

	err = fetchOnlineAggregates(ctx, tier, unique, batch, ret, numSlotsLeft)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func fetchOfflineAggregates(tier tier.Tier, aggMap map[ftypes.AggName]aggregate.Aggregate, batch []aggregate.GetAggValueRequest, ret []value.Value) (int, error) {
	offlinePtr := make([]int, 0, len(batch))
	namespaces := make([]string, 0, len(batch))
	identifier := make([]string, 0, len(batch))
	offlineKeys := make([]value.Value, 0, len(batch))
	numSlotsLeft := len(batch)

	// Fetch offline aggregate values
	for i, req := range batch {
		agg := aggMap[req.AggName]
		if !agg.IsOffline() {
			continue
		}
		offlinePtr = append(offlinePtr, i)
		duration, err := getDuration(req.Kwargs)
		if err != nil {
			return numSlotsLeft, err
		}
		aggPhaserIdentifier := fmt.Sprintf("%s-%d", agg.Name, duration)
		namespaces = append(namespaces, OFFLINE_AGG_NAMESPACE)
		identifier = append(identifier, aggPhaserIdentifier)
		offlineKeys = append(offlineKeys, req.Key)
	}

	if len(offlinePtr) > 0 {
		offlineValues, err := phaser.BatchGet(tier, namespaces, identifier, offlineKeys)
		if err != nil {
			return numSlotsLeft, err
		}

		for i, v := range offlineValues {
			ret[offlinePtr[i]] = v
			numSlotsLeft -= 1
		}
	}

	return numSlotsLeft, nil
}

func fetchForeverAggregates(ctx context.Context, tier tier.Tier, aggMap map[ftypes.AggName]aggregate.Aggregate, batch []aggregate.GetAggValueRequest, ret []value.Value, numSlotsLeft int) (int, error) {
	foreverPtr := make([]int, 0, numSlotsLeft)
	foreverKeys := make([]value.Value, 0, numSlotsLeft)
	var foreverAgg aggregate.Aggregate
	var foreverKwarags value.Dict

	// Fetch forever aggregates, ( these dont need histograms )
	for i, req := range batch {
		agg := aggMap[req.AggName]
		if !agg.IsForever() {
			continue
		}

		// Current code only supports knn, need to extend this to support other aggregates
		if agg.Options.AggType != "knn" {
			return numSlotsLeft, fmt.Errorf("error: Only KNN supports forever aggregates")
		}
		foreverPtr = append(foreverPtr, i)
		foreverKeys = append(foreverKeys, req.Key)
		// Currently we assume the aggregate and kwarg is the same for all knn requests.
		foreverAgg = agg
		foreverKwarags = req.Kwargs
	}

	if len(foreverPtr) > 0 {
		nn, err := tier.MilvusClient.GetNeighbors(ctx, foreverAgg, foreverKeys, foreverKwarags)
		if err != nil {
			return numSlotsLeft, err
		}
		for j, v := range nn {
			ret[foreverPtr[j]] = v
			numSlotsLeft -= 1
		}
	}

	return numSlotsLeft, nil
}

func fetchOnlineAggregates(ctx context.Context, tier tier.Tier, aggMap map[ftypes.AggName]aggregate.Aggregate, batch []aggregate.GetAggValueRequest, ret []value.Value, numSlotsLeft int) error {
	histograms := make([]modelCounter.Histogram, 0, numSlotsLeft)
	ids := make([]ftypes.AggId, 0, numSlotsLeft)
	keys := make([]value.Value, 0, numSlotsLeft)
	kwargs := make([]value.Dict, 0, numSlotsLeft)

	var onlinePtr []int
	// Fetch online aggregate values
	for i, req := range batch {
		agg := aggMap[req.AggName]
		if agg.IsForever() || agg.IsOffline() {
			continue
		}
		onlinePtr = append(onlinePtr, i)
		h, err := modelCounter.ToHistogram(agg.Id, agg.Options)
		if err != nil {
			return fmt.Errorf("failed to make histogram from aggregate at index %d of batch: %v", i, err)
		}
		histograms = append(histograms, h)
		ids = append(ids, agg.Id)
		keys = append(keys, req.Key)
		kwargs = append(kwargs, req.Kwargs)
	}

	if len(onlinePtr) > 0 {
		onlineValues, err := counter.BatchValue(ctx, tier, ids, keys, histograms, kwargs)
		if err != nil {
			return err
		}

		for i, v := range onlineValues {
			ret[onlinePtr[i]] = v
		}
	}
	return nil
}

// Update the aggregates given a kafka consumer responsible for reading any stream
func Update[I action.Action | profile.ProfileItem](ctx context.Context, tier tier.Tier, items []I, agg aggregate.Aggregate) error {
	table, err := transform(tier, items, agg.Query)
	if err != nil {
		return fmt.Errorf("failed to transform actions: %w", err)
	}
	tier.Logger.Info("Processed aggregate",
		zap.String("name", string(agg.Name)),
		zap.Int("input", len(items)),
		zap.Int("output", table.Len()))
	// Update the aggregate according to the type
	if agg.IsOffline() { // Offline Aggregates
		tier.Logger.Info(fmt.Sprintf("found %d new items, %d transformed %s for offline aggregate: %s", len(items), table.Len(), agg.Source, agg.Name))
		offlineTransformProducer := tier.Producers[libcounter.AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME]
		for i := 0; i < table.Len(); i++ {
			rowVal, _ := table.At(i)
			rowDict, _ := rowVal.(value.Dict)
			dict := value.NewDict(map[string]value.Value{
				"aggregate": value.String(agg.Name),
				"groupkey":  rowDict.GetUnsafe("groupkey"),
				"value":     rowDict.GetUnsafe("value"),
				"timestamp": rowDict.GetUnsafe("timestamp"),
			})
			err = offlineTransformProducer.Log(ctx, value.ToJSON(dict), nil)
			if err != nil {
				tier.Logger.Error(fmt.Sprintf("failed to log action proto: %v", err))
			}
		}
		return nil
	} else if agg.IsForever() {
		// Forever Aggregates dont use histograms
		// Current support for only KNN, add support for other aggregates
		// https://linear.app/fennel-ai/issue/REX-1053/support-forever-aggregates
		if agg.Options.AggType != "knn" {
			return fmt.Errorf("forever aggregates are not supported for aggregate %s", agg.Name)
		}
		// Update the aggregate
		// Use milvus library to update the index with all actions
		err = tier.MilvusClient.InsertStream(ctx, agg, table)
		if err != nil {
			return fmt.Errorf("failed to insert stream into milvus: %w", err)
		}
		return nil
	} else { // Online duration based aggregates
		histogram, err := modelCounter.ToHistogram(agg.Id, agg.Options)
		if err != nil {
			return fmt.Errorf("failed to make histogram from aggregate: %w", err)
		}
		if err = counter.Update(ctx, tier, agg, table, histogram); err != nil {
			return fmt.Errorf("failed to update counter: %w", err)
		}
		return err
	}
}

// ============================
// Private helpers below
// ============================

func transform(tier tier.Tier, items any, query ast.Ast) (value.List, error) {
	bootargs := bootarg.Create(tier)
	executor := engine.NewQueryExecutor(bootargs)
	var table value.List
	var err error
	var key string
	switch t := items.(type) {
	case []action.Action:
		key = "actions"
		table, err = action.ToList(t)
	case []profile.ProfileItem:
		key = "profiles"
		table, err = profile.ToList(t)
	default:
		return table, fmt.Errorf("unsupported type: %T", t)
	}
	if err != nil {
		return value.NewList(), err
	}
	result, err := executor.Exec(context.Background(), query, value.NewDict(map[string]value.Value{key: table}))
	if err != nil {
		return value.NewList(), err
	}
	var ok bool
	table, ok = result.(value.List)
	if !ok {
		return value.NewList(), fmt.Errorf("query did not transform items into a list")
	}
	return table, nil
}

// TODO: Use AggId here as well to keep the formatting consistent with remote storage (MemoryDB)
func makeCacheKey(name ftypes.AggName, key value.Value, kwargs value.Dict) string {
	return fmt.Sprintf("%d:%s:%s:%s", cacheVersion, name, key.String(), kwargs.String())
}

func fromCacheValue(tier tier.Tier, v interface{}) (value.Value, bool) {
	switch v := v.(type) {
	case value.Value:
		return v, true
	default:
		// log unexpected error
		err := fmt.Errorf("value not of type value.Value: %v", v)
		tier.Logger.Error("aggregate value cache error: ", zap.Error(err))
		return nil, false
	}
}
