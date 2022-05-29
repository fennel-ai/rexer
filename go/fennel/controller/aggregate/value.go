package aggregate

import (
	"context"
	"fennel/controller/action"
	"fmt"
	"time"

	"fennel/controller/counter"
	"fennel/controller/profile"
	"fennel/engine"
	"fennel/engine/ast"
	"fennel/engine/interpreter/bootarg"
	"fennel/kafka"
	libaction "fennel/lib/action"
	"fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/phaser"
	profilelib "fennel/lib/profile"
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
	ckey := makeCacheKey(name, key, kwargs)
	v, ok := tier.PCache.Get(ckey)
	// If already present in cache and no failure interpreting it, return directly
	if ok {
		if val, ok := fromCacheValue(tier, v); ok {
			return val, nil
		}
	}
	// otherwise compute value and store in cache
	val, err := unitValue(ctx, tier, name, key, kwargs)
	if err != nil {
		return nil, err
	}
	ok = tier.PCache.SetWithTTL(ckey, val, int64(len(ckey)+len(val.String())), cacheValueDuration)
	if !ok {
		tier.Logger.Debug(fmt.Sprintf("failed to set aggregate value in cache: key: '%s' value: '%s'", ckey, val.String()))
	}
	return val, nil
}

func BatchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	ret := make([]value.Value, len(batch))
	uckeys := make([]string, len(batch))
	uncachedReqs := make([]aggregate.GetAggValueRequest, len(batch))
	ptr := make([]int, len(batch))
	seen := make(map[string]int, len(batch))

	j := 0
	for i, req := range batch {
		ckey := makeCacheKey(req.AggName, req.Key, req.Kwargs)
		if v, ok := tier.PCache.Get(ckey); ok {
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
		if ok := tier.PCache.SetWithTTL(uckeys[idx], ucvals[idx], int64(len(uckeys[idx])+len(ucvals[idx].String())), cacheValueDuration); !ok {
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
	histogram, err := modelCounter.ToHistogram(tier, agg.Id, agg.Options)
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
			return nil, fmt.Errorf("failed to retrieve aggregate %s ", name)
		}
	}
	var offlinePtr []int
	var namespaces []string
	var identifier []string
	var offlineKeys []value.String
	// Fetch offline aggregate values
	for i, req := range batch {
		agg := unique[req.AggName]
		if agg.Options.CronSchedule == "" {
			continue
		}
		offlinePtr = append(offlinePtr, i)
		duration, err := getDuration(req.Kwargs)
		if err != nil {
			return nil, err
		}
		aggPhaserIdentifier := fmt.Sprintf("%s-%d", agg.Name, duration)
		namespaces = append(namespaces, OFFLINE_AGG_NAMESPACE)
		identifier = append(identifier, aggPhaserIdentifier)
		// Convert all keys to value.String
		if s, ok := req.Key.(value.String); ok {
			offlineKeys = append(offlineKeys, s)
		} else {
			offlineKeys = append(offlineKeys, value.String(req.Key.String()))
		}
	}

	ret := make([]value.Value, n)

	if len(offlinePtr) > 0 {
		offlineValues, err := phaser.BatchGet(tier, namespaces, identifier, offlineKeys)
		if err != nil {
			return nil, err
		}

		for i, v := range offlineValues {
			ret[offlinePtr[i]] = v
		}
	}

	// Fetch forever aggregates, ( these dont need histograms )
	for i, req := range batch {
		agg := unique[req.AggName]
		if agg.Options.CronSchedule != "" {
			continue
		}
		onlinePtr = append(onlinePtr, i)
		histograms[i], err = modelCounter.ToHistogram(tier, agg.Id, agg.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to make histogram from aggregate at index %d of batch: %v", i, err)
		}
		ids[i] = agg.Id
		keys[i] = req.Key
		kwargs[i] = req.Kwargs
	}

	numOnline := n - len(offlinePtr)
	histograms := make([]modelCounter.Histogram, numOnline)
	ids := make([]ftypes.AggId, numOnline)
	keys := make([]value.Value, numOnline)
	kwargs := make([]value.Dict, numOnline)

	var onlinePtr []int
	// Fetch online aggregate values
	for i, req := range batch {
		agg := unique[req.AggName]
		if agg.Options.CronSchedule != "" {
			continue
		}
		onlinePtr = append(onlinePtr, i)
		histograms[i], err = modelCounter.ToHistogram(tier, agg.Id, agg.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to make histogram from aggregate at index %d of batch: %v", i, err)
		}
		ids[i] = agg.Id
		keys[i] = req.Key
		kwargs[i] = req.Kwargs
	}

	if len(onlinePtr) > 0 {
		onlineValues, err := counter.BatchValue(ctx, tier, ids, keys, histograms, kwargs)
		if err != nil {
			return nil, err
		}

		for i, v := range onlineValues {
			ret[onlinePtr[i]] = v
		}
	}
	return ret, nil
}

// Update the aggregates given a kafka consumer responsible for reading any stream
func Update(ctx context.Context, tier tier.Tier, consumer kafka.FConsumer, agg aggregate.Aggregate) error {
	var table value.List
	var err error
	var streamLen int

	if agg.Source == aggregate.SOURCE_PROFILE {
		profiles, err := profile.ReadBatch(ctx, consumer, 20000, time.Second*10)

		if err != nil {
			return err
		}
		if len(profiles) == 0 {
			return nil
		}

		table, err = transformProfiles(tier, profiles, agg.Query)
		streamLen = len(profiles)
	} else {
		actions, err := action.ReadBatch(ctx, consumer, 20000, time.Second*10)
		if err != nil {
			return err
		}
		if len(actions) == 0 {
			return nil
		}

		table, err = transformActions(tier, actions, agg.Query)
		streamLen = len(actions)
	}

	if err != nil {
		return err
	}

	if table.Len() == 0 {
		tier.Logger.Info(fmt.Sprintf("no data to update aggregate %s", agg.Name))
		_, err = consumer.Commit()
		return err
	}

	// Update the aggregate according to the type

	// Offline Aggregates
	if agg.IsOffline() {
		tier.Logger.Info(fmt.Sprintf("found %d new %s, %d transformed %s for offline aggregate: %s", streamLen, agg.Source, table.Len(), agg.Source, agg.Name))

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
		_, err = consumer.Commit()
		return err
	}

	// Forever Aggregates dont user histograms
	if agg.Options.Durations == nil || len(agg.Options.Durations) == 0 {
		// Current support for only KNN, add support for other aggregates
		// https://linear.app/fennel-ai/issue/REX-1053/support-forever-aggregates
		if agg.Name != "knn" {
			return fmt.Errorf("forever aggregates are not supported for aggregate %s", agg.Name)
		}

		// Update the aggregate
		// Use milvus library to update the index with all actions

		tier.Logger.Info(fmt.Sprintf("found %d new %s, %d transformed %s for forever aggregate: %s", streamLen, agg.Source, table.Len(), agg.Source, agg.Name))
		return counter.Batch(ctx, tier, agg.Id, table)
	}

	// Online duration based aggregates

	tier.Logger.Info(fmt.Sprintf("found %d new %s, %d transformed %s for online aggregate: %s", streamLen, agg.Source, table.Len(), agg.Source, agg.Name))
	histogram, err := modelCounter.ToHistogram(tier, agg.Id, agg.Options)
	if err != nil {
		return err
	}
	if err = counter.Update(ctx, tier, agg, table, histogram); err != nil {
		return err
	}
	_, err = consumer.Commit()
	return err
}

// ============================
// Private helpers below
// ============================

func transformProfiles(tier tier.Tier, profiles []profilelib.ProfileItem, query ast.Ast) (value.List, error) {
	bootargs := bootarg.Create(tier)
	executor := engine.NewQueryExecutor(bootargs)
	table, err := profilelib.ToList(profiles)
	if err != nil {
		return value.NewList(), err
	}

	result, err := executor.Exec(context.Background(), query, value.NewDict(map[string]value.Value{"profiles": table}))
	if err != nil {
		return value.NewList(), err
	}
	table, ok := result.(value.List)
	if !ok {
		return value.NewList(), fmt.Errorf("query did not transform profiles into a list")
	}
	return table, nil
}

func transformActions(tier tier.Tier, actions []libaction.Action, query ast.Ast) (value.List, error) {
	bootargs := bootarg.Create(tier)
	executor := engine.NewQueryExecutor(bootargs)
	table, err := libaction.ToList(actions)
	if err != nil {
		return value.NewList(), err
	}

	result, err := executor.Exec(context.Background(), query, value.NewDict(map[string]value.Value{"actions": table}))
	if err != nil {
		return value.NewList(), err
	}
	table, ok := result.(value.List)
	if !ok {
		return value.NewList(), fmt.Errorf("query did not transform actions into a list")
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
