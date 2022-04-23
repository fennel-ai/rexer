package aggregate

import (
	"context"
	"fmt"
	"time"

	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	modelAgg "fennel/model/aggregate"
	"fennel/tier"

	"google.golang.org/protobuf/proto"
)

func Store(ctx context.Context, tier tier.Tier, agg aggregate.Aggregate) error {
	if err := agg.Validate(); err != nil {
		return err
	}

	// Check if agg already exists in db
	agg2, err := Retrieve(ctx, tier, agg.Name)
	// Only error that should happen is when agg is not present
	if err != nil && err != aggregate.ErrNotFound {
		return err
	} else if err == nil {
		// if already present, check if query and options are the same
		// if they are the same, do nothing
		// if they are different, return error
		if agg.Query.Equals(agg2.Query) && agg.Options.Equals(agg2.Options) {
			return nil
		} else {
			return fmt.Errorf("already present but with different query/options")
		}
	}

	if agg.Options.CronSchedule != "" {
		// If offline aggregate, write to AWS Glue
	}

	querySer, err := ast.Marshal(agg.Query)
	if err != nil {
		return fmt.Errorf("can not marshal aggregate query: %v", err)
	}
	optionSer, err := proto.Marshal(aggregate.ToProtoOptions(agg.Options))
	if err != nil {
		return fmt.Errorf("can not marshal aggregate options: %v", err)
	}
	if agg.Timestamp == 0 {
		agg.Timestamp = ftypes.Timestamp(time.Now().Unix())
	}

	return modelAgg.Store(ctx, tier, agg.Name, querySer, agg.Timestamp, optionSer)
}

func Retrieve(ctx context.Context, tier tier.Tier, aggname ftypes.AggName) (aggregate.Aggregate, error) {
	empty := aggregate.Aggregate{}
	if len(aggname) == 0 {
		return empty, fmt.Errorf("aggregate name can not be of length zero")
	}
	aggser, err := modelAgg.Retrieve(ctx, tier, aggname)
	if err != nil {
		return empty, err
	}
	return aggregate.FromAggregateSer(aggser)
}

// RetrieveAll returns all aggregates
func RetrieveAll(ctx context.Context, tier tier.Tier) ([]aggregate.Aggregate, error) {
	retSer, err := modelAgg.RetrieveAll(ctx, tier)
	if err != nil {
		return nil, err
	}
	ret := make([]aggregate.Aggregate, len(retSer))
	for i, ser := range retSer {
		ret[i], err = aggregate.FromAggregateSer(ser)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func Deactivate(ctx context.Context, tier tier.Tier, aggname ftypes.AggName) error {
	if len(aggname) == 0 {
		return fmt.Errorf("aggregate name can not be of length zero")
	}
	// Check if agg already exists in db
	aggser, err := modelAgg.RetrieveNoFilter(ctx, tier, aggname)
	// If it is absent, it returns aggregate.ErrNotFound
	// If any other error, return it as well
	if err != nil {
		return err
	}
	// If it is present and inactive, do nothing
	// otherwise, deactivate
	if !aggser.Active {
		return nil
	} else {
		err := modelAgg.Deactivate(ctx, tier, aggname)
		return err
	}
}
