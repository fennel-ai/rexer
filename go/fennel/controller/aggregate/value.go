package aggregate

import (
	"context"
	"fmt"
	"time"

	"fennel/controller/action"
	"fennel/controller/counter"
	"fennel/engine"
	"fennel/engine/ast"
	"fennel/engine/interpreter/bootarg"
	"fennel/kafka"
	libaction "fennel/lib/action"
	"fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	modelCounter "fennel/model/counter"
	"fennel/tier"

	"go.uber.org/zap"
)

const cacheValueDuration = time.Minute

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
	ok = tier.PCache.SetWithTTL(ckey, val, cacheValueDuration)
	if !ok {
		tier.Logger.Info(
			fmt.Sprintf("failed to set aggregate value in cache: key: '%s' value: '%s'", ckey, val.String()),
		)
	}
	return val, nil
}

func BatchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	ckeys := make([]string, 0, len(batch))
	for _, req := range batch {
		k := makeCacheKey(req.AggName, req.Key, req.Kwargs)
		ckeys = append(ckeys, k)
	}
	vals := make([]value.Value, len(batch))
	found := make([]bool, len(batch))
	for i, ckey := range ckeys {
		var v interface{}
		v, ok := tier.PCache.Get(ckey)
		if ok {
			val, ok := fromCacheValue(tier, v)
			vals[i] = val
			found[i] = ok
		}
	}
	ret := make([]value.Value, len(batch))
	var ptr []int
	var uckeys []string
	var uncachedReqs []aggregate.GetAggValueRequest
	// filter out requests not present in cache
	for i, v := range vals {
		if !found[i] {
			// couldn't get from cache so filter it out
			uckeys = append(uckeys, ckeys[i])
			uncachedReqs = append(uncachedReqs, batch[i])
			ptr = append(ptr, i)
		} else {
			ret[i] = v
		}
	}
	ucvals, err := batchValue(ctx, tier, uncachedReqs)
	if err != nil {
		return nil, err
	}
	for i, ucv := range ucvals {
		ret[ptr[i]] = ucv
	}
	// now set uncached values in cache
	for i := range uckeys {
		ok := tier.PCache.SetWithTTL(uckeys[i], ucvals[i], cacheValueDuration)
		if !ok {
			tier.Logger.Info(fmt.Sprintf(
				"failed to set aggregate value in cache: key: '%s' value: '%s'", uckeys[i], ucvals[i].String(),
			))
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
	histogram, err := modelCounter.ToHistogram(agg.Options)
	if err != nil {
		return nil, err
	}
	return counter.Value(ctx, tier, agg.Id, key, histogram, kwargs)
}

func batchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	n := len(batch)
	histograms := make([]modelCounter.Histogram, n)
	ids := make([]ftypes.AggId, n)
	keys := make([]value.Value, n)
	kwargs := make([]value.Dict, n)
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
	for i, req := range batch {
		agg := unique[req.AggName]
		histograms[i], err = modelCounter.ToHistogram(agg.Options)
		if err != nil {
			return nil, fmt.Errorf("failed to make histogram from aggregate at index %d of batch: %v", i, err)
		}
		ids[i] = agg.Id
		keys[i] = req.Key
		kwargs[i] = req.Kwargs
	}
	return counter.BatchValue(ctx, tier, ids, keys, histograms, kwargs)
}

func Update(ctx context.Context, tier tier.Tier, consumer kafka.FConsumer, agg aggregate.Aggregate) error {
	actions, err := action.ReadBatch(ctx, consumer, 20000, time.Second*10)
	if err != nil {
		return err
	}
	if len(actions) == 0 {
		return nil
	}
	table, err := transformActions(tier, actions, agg.Query)

	if err != nil {
		return err
	}

	// Offline Aggregates
	if agg.IsOffline() {
		tier.Logger.Info(fmt.Sprintf("found %d new actions, %d transformed actions for offline aggregate: %s", len(actions), table.Len(), agg.Name))

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
	tier.Logger.Info(fmt.Sprintf("found %d new actions for online aggregate: %s", len(actions), agg.Name))

	histogram, err := modelCounter.ToHistogram(agg.Options)
	if err != nil {
		return err
	}
	if err = counter.Update(ctx, tier, agg, table, histogram); err != nil {
		return err
	}
	_, err = consumer.Commit()
	return err
}

//============================
// Private helpers below
//============================

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
