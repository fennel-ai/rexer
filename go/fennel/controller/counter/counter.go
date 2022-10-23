package counter

import (
	"context"

	"fennel/lib/arena"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/tier"
)

func Value(ctx context.Context, tier tier.Tier, aggId ftypes.AggId, key value.Value, kwargs value.Dict,
) (value.Value, error) {
	vals, err := BatchValue(ctx, tier,
		[]ftypes.AggId{aggId}, []value.Value{key}, []value.Dict{kwargs})
	if err != nil {
		return value.Nil, err
	}
	return vals[0], nil
}

func BatchValue(ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, keys []value.Value, kwargs []value.Dict,
) ([]value.Value, error) {
	ret := make([]value.Value, len(aggIds))
	ctx, t := timer.Start(ctx, tier.ID, "counter.NitrousBatchValue")
	defer t.Stop()
	idxByAgg := make(map[ftypes.AggId][]int)
	for i, aggId := range aggIds {
		idxByAgg[aggId] = append(idxByAgg[aggId], i)
	}
	// Note: we make the calls serially because for the most part, we will only
	// have one aggregate per call.
	// TODO(abhay): Call nitrous in parallel if/when we have multiple aggregates.
	for aggId, indices := range idxByAgg {
		aggkeys := arena.Values.Alloc(len(indices), len(indices))
		defer arena.Values.Free(aggkeys)
		aggkwargs := arena.DictValues.Alloc(len(indices), len(indices))
		defer arena.DictValues.Free(aggkwargs)
		for i, index := range indices {
			aggkeys[i] = keys[index]
			aggkwargs[i] = kwargs[index]
		}
		output := arena.Values.Alloc(len(indices), len(indices))
		defer arena.Values.Free(output)

		// TODO(mohit): We should send the Get request based on the groupkey ('aggkeys') since the binlog is sharded
		err := tier.NitrousClient.GetMulti(ctx, aggId, aggkeys, aggkwargs, output)
		if err != nil {
			return ret, err
		}
		for i, index := range indices {
			ret[index] = output[i]
		}
	}
	return ret, nil
}

func Update(
	ctx context.Context, tier tier.Tier, aggId ftypes.AggId, table value.List) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "counter.update")
	defer tmr.Stop()
	return tier.NitrousClient.Push(ctx, aggId, table)
}
