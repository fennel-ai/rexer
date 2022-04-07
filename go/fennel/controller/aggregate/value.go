package aggregate

import (
	"context"
	"fmt"
	"time"

	"fennel/controller/action"
	"fennel/controller/counter"
	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/kafka"
	libaction "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	modelCounter "fennel/model/counter"
	"fennel/tier"
	"github.com/dgraph-io/ristretto"
	"go.uber.org/zap"
)

const (
	cacheVersion       = 0 // increment this to invalidate all existing cache keys for aggregate
	cacheValueDuration = time.Minute
)

var cache *ristretto.Cache

func init() {
	var err error
	cache, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: 10 * (1 << 20), // expecting to store 1 million unique items in full cache
		MaxCost:     1 << 25,        // 32 MB
		BufferItems: 64,             // ristretto recommends keeping this at 64
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create aggregate value cache"))
	}
}

func Value(
	ctx context.Context, tier tier.Tier, name ftypes.AggName, key value.Value, kwargs value.Dict,
) (value.Value, error) {
	ckey := makeCacheKey(name, key, kwargs)
	v, ok := cache.Get(ckey)
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
	ok = cache.SetWithTTL(ckey, val, 0, cacheValueDuration)
	if !ok {
		tier.Logger.Warn(
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
		v, ok := cache.Get(ckey)
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
	var ucvalsSer []interface{}
	for i, ucv := range ucvals {
		ret[ptr[i]] = ucv
		ucvalsSer = append(ucvalsSer, ucv.String())
	}
	// now set uncached values in cache
	for i := range uckeys {
		ok := cache.SetWithTTL(uckeys[i], ucvals[i], 0, cacheValueDuration)
		if !ok {
			tier.Logger.Warn(fmt.Sprintf(
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
	histogram, err := toHistogram(agg)
	if err != nil {
		return nil, err
	}
	return counter.Value(ctx, tier, name, key, histogram, kwargs)
}

func batchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	n := len(batch)
	histograms := make([]modelCounter.Histogram, n)
	names := make([]ftypes.AggName, n)
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
		histograms[i], err = toHistogram(unique[req.AggName])
		if err != nil {
			return nil, fmt.Errorf("failed to make histogram from aggregate at index %d of batch: %v", i, err)
		}
		names[i] = req.AggName
		keys[i] = req.Key
		kwargs[i] = req.Kwargs
	}
	return counter.BatchValue(ctx, tier, names, keys, histograms, kwargs)
}

func Update(ctx context.Context, tier tier.Tier, consumer kafka.FConsumer, agg aggregate.Aggregate) error {
	actions, err := action.ReadBatch(ctx, consumer, 20000, time.Second*10)
	if err != nil {
		return err
	}
	tier.Logger.Info(fmt.Sprintf("found %d new actions for aggregate: %s", len(actions), agg.Name))
	if len(actions) == 0 {
		return nil
	}
	table, err := transformActions(tier, actions, agg.Query)

	if err != nil {
		return err
	}
	histogram, err := toHistogram(agg)
	if err != nil {
		return err
	}
	if err = counter.Update(ctx, tier, agg.Name, table, histogram); err != nil {
		return err
	}
	return consumer.Commit()
}

//============================
// Private helpers below
//============================

func transformActions(tier tier.Tier, actions []libaction.Action, query ast.Ast) (value.List, error) {
	bootargs := bootarg.Create(tier)
	interpreter := interpreter.NewInterpreter(bootargs)
	table, err := libaction.ToList(actions)
	if err != nil {
		return value.NewList(), err
	}

	result, err := interpreter.Eval(query, value.NewDict(map[string]value.Value{"actions": table}))
	if err != nil {
		return value.NewList(), err
	}
	table, ok := result.(value.List)
	if !ok {
		return value.NewList(), fmt.Errorf("query did not transform actions into a list")
	}
	return table, nil
}

func toHistogram(agg aggregate.Aggregate) (modelCounter.Histogram, error) {
	switch agg.Options.AggType {
	case "sum":
		return modelCounter.NewSum(agg.Name, agg.Options.Durations), nil
	case "timeseries_sum":
		return modelCounter.NewTimeseriesSum(agg.Name, agg.Options.Window, agg.Options.Limit), nil
	case "average":
		return modelCounter.NewAverage(agg.Name, agg.Options.Durations), nil
	case "list":
		return modelCounter.NewList(agg.Name, agg.Options.Durations), nil
	case "min":
		return modelCounter.NewMin(agg.Name, agg.Options.Durations), nil
	case "max":
		return modelCounter.NewMax(agg.Name, agg.Options.Durations), nil
	case "stddev":
		return modelCounter.NewStdDev(agg.Name, agg.Options.Durations), nil
	case "rate":
		return modelCounter.NewRate(agg.Name, agg.Options.Durations, agg.Options.Normalize), nil
	case "topk":
		return modelCounter.NewTopK(agg.Name, agg.Options.Durations), nil
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", agg.Options.AggType)
	}
}

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
