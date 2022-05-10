package aggregate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	modelAgg "fennel/model/aggregate"
	"fennel/tier"
)

func Store(ctx context.Context, tier tier.Tier, agg aggregate.Aggregate) error {
	if err := agg.Validate(); err != nil {
		return err
	}

	// Check if agg already exists in db
	agg2, err := Retrieve(ctx, tier, agg.Name)
	// Only error that should happen is when agg is not present
	if err != nil && !errors.Is(err, aggregate.ErrNotFound) {
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
	if agg.IsOffline() {
		// If offline aggregate, write to AWS Glue
		err := tier.GlueClient.ScheduleOfflineAggregate(tier.ID, agg)
		if err != nil {
			return err
		}
	}
	if agg.Timestamp == 0 {
		agg.Timestamp = ftypes.Timestamp(time.Now().Unix())
	}
	agg.Active = true
	return modelAgg.Store(ctx, tier, agg)
}

func Retrieve(ctx context.Context, tier tier.Tier, aggname ftypes.AggName) (aggregate.Aggregate, error) {
	empty := aggregate.Aggregate{}
	if len(aggname) == 0 {
		return empty, fmt.Errorf("aggregate name can not be of length zero")
	}
	var agg aggregate.Aggregate
	if def, ok := tier.AggregateDefs.Load(aggname); !ok {
		var err error
		agg, err = modelAgg.Retrieve(ctx, tier, aggname)
		if err != nil {
			return empty, fmt.Errorf("failed to get aggregate: %w", err)
		}
		tier.AggregateDefs.Store(aggname, agg)
	} else {
		agg = def.(aggregate.Aggregate)
	}
	return agg, nil
}

// RetrieveActive returns all active aggregates
func RetrieveActive(ctx context.Context, tier tier.Tier) ([]aggregate.Aggregate, error) {
	return modelAgg.RetrieveActive(ctx, tier)
}

func Deactivate(ctx context.Context, tier tier.Tier, aggname ftypes.AggName) error {
	if len(aggname) == 0 {
		return fmt.Errorf("aggregate name can not be of length zero")
	}
	// Remove if present in cache
	tier.AggregateDefs.Delete(aggname)
	// Check if agg already exists in db
	agg, err := modelAgg.RetrieveNoFilter(ctx, tier, aggname)
	// If it is absent, it returns aggregate.ErrNotFound
	// If any other error, return it as well
	if err != nil {
		return err
	}
	// If it is present and inactive, do nothing
	// otherwise, deactivate
	if !agg.Active {
		return nil
	} else {
		// deactive trigger only if the aggregate if offline
		if agg.IsOffline() {
			if err := tier.GlueClient.DeactivateOfflineAggregate(string(aggname)); err != nil {
				return err
			}
		}
		// Disable online & offline aggregates
		err = modelAgg.Deactivate(ctx, tier, aggname)
		return err
	}
}
