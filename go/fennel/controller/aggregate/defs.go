package aggregate

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/phaser"
	modelAgg "fennel/model/aggregate"
	"fennel/tier"

	"google.golang.org/protobuf/proto"
)

func getUpdateFrequency(cron string) time.Duration {
	parts := strings.Split(cron, " ")
	if strings.Contains(parts[1], "/") {
		x, _ := strconv.Atoi(strings.Replace(parts[1], "*/", "", 1))
		return time.Duration(x) * time.Hour
	} else if strings.Contains(parts[2], "/") {
		x, _ := strconv.Atoi(strings.Replace(parts[2], "*/", "", 1))
		return time.Duration(x) * time.Hour * 24
	}
	return 0
}

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
		for _, duration := range agg.Options.Durations {
			prefix := fmt.Sprintf("t_%d/%s-%d", int(tier.ID), agg.Name, duration)
			aggPhaserIdentifier := fmt.Sprintf("%s-%d", agg.Name, duration)
			ttl := getUpdateFrequency(agg.Options.CronSchedule) * 3
			err = phaser.NewPhaser("agg", aggPhaserIdentifier, tier.Args.OfflineAggBucket, prefix, ttl, phaser.ITEM_SCORE_LIST, tier)
			if err != nil {
				return err
			}
		}
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
	var agg aggregate.Aggregate
	if def, ok := tier.AggregateDefs.Load(aggname); !ok {
		aggser, err := modelAgg.Retrieve(ctx, tier, aggname)
		if err != nil {
			return empty, fmt.Errorf("failed to get aggregate: %w", err)
		}
		agg, err = aggregate.FromAggregateSer(aggser)
		if err != nil {
			return empty, fmt.Errorf("failed to deserialize aggregate: %w", err)
		}
		tier.AggregateDefs.Store(aggname, agg)
	} else {
		agg = def.(aggregate.Aggregate)
	}
	return agg, nil
}

// RetrieveActive returns all active aggregates
func RetrieveActive(ctx context.Context, tier tier.Tier) ([]aggregate.Aggregate, error) {
	retSer, err := modelAgg.RetrieveActive(ctx, tier)
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
	// Remove if present in cache
	tier.AggregateDefs.Delete(aggname)
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
		// deactive trigger only if the aggregate if offline
		agg, err := aggregate.FromAggregateSer(aggser)
		if err != nil {
			return err
		}
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
