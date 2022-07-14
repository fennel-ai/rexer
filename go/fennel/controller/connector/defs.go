package connector

import (
	"context"
	"errors"
	"fennel/lib/aggregate"
	"fennel/lib/connector"
	modelAgg "fennel/model/aggregate"
	connectorModel "fennel/model/connector"
	"fennel/tier"
	"fmt"
)

func Store(ctx context.Context, tier tier.Tier, conn connector.Connector) error {
	if err := conn.Validate(); err != nil {
		return err
	}
	// Check if agg already exists in db
	agg2, err := connectorModel.Retrieve(ctx, tier, agg.Name)
	if err != nil {
		if errors.Is(err, aggregate.ErrNotFound) {
			tier.Logger.Debug("Storing new connector")
			agg.Active = true
			return modelAgg.Store(ctx, tier, agg)
		} else {
			return fmt.Errorf("failed to retrieve aggregate: %w", err)
		}
	} else {
		// if already present, check if query and options are the same
		// if they are the same, activate the aggregate in case it was deactivated.
		// if they are different, return error
		if agg.Query.Equals(agg2.Query) && agg.Options.Equals(agg2.Options) && agg.Source == agg2.Source {
			if !agg2.Active {
				err := modelAgg.Activate(ctx, tier, agg.Name)
				if err != nil {
					return fmt.Errorf("failed to reactivate aggregate '%s': %w", agg.Name, err)
				}
			}
			return nil
		} else {
			return fmt.Errorf("already present but with different query/options")
		}
	}
}
