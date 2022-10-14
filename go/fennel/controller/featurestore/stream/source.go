package stream

import (
	"context"
	"errors"
	"fmt"

	"fennel/featurestore/tier"
	lib "fennel/lib/featurestore/stream"
	"fennel/model/featurestore/stream"
)

func StoreSource(ctx context.Context, tier tier.Tier, src lib.Source) error {
	if err := src.Validate(); err != nil {
		return err
	}
	// Check if source already exists in db
	src2, err := stream.RetrieveSource(ctx, tier, src.GetSourceName())
	if err != nil {
		if errors.Is(err, lib.ErrSrcNotFound) {
			tier.Logger.Debug("Storing new src " + src.GetSourceName())

			// Write the source to Airbyte
			if tier.AirbyteClient.IsAbsent() {
				return fmt.Errorf("error: Airbyte client is not initialized")
			}
			diSrc, err := toDataIntegrationSource(src)
			if err != nil {
				return fmt.Errorf("error: failed to convert stream.Source to data_integration.Source: %w", err)
			}
			srcId, err := tier.AirbyteClient.MustGet().CreateSource(diSrc)
			if err != nil {
				return fmt.Errorf("error: failed to create source: %w", err)
			}

			// Write the source to the db
			return stream.StoreSource(ctx, tier, src, srcId)
		} else {
			return fmt.Errorf("failed to retrieve source: %w", err)
		}
	}

	err = src.Equals(src2)
	if err != nil {
		return fmt.Errorf("source already present but with different params : %w", err)
	}
	return nil
}

func DeleteSource(ctx context.Context, tier tier.Tier, name string) error {
	if err := stream.CheckIfInUse(ctx, tier, name); err != nil {
		return err
	}
	src, err := stream.RetrieveSource(ctx, tier, name)
	if err != nil {
		return fmt.Errorf("failed to retrieve source: %w", err)
	}
	if tier.AirbyteClient.IsAbsent() {
		return fmt.Errorf("error: Airbyte client is not initialized")
	}
	diSrc, err := toDataIntegrationSource(src)
	if err != nil {
		return fmt.Errorf("error: failed to convert stream.Source to data_integration.Source: %w", err)
	}
	if err = tier.AirbyteClient.MustGet().DeleteSource(diSrc); err != nil {
		return fmt.Errorf("error: failed to delete source: %w", err)
	}
	return stream.DeleteSource(ctx, tier, src)
}
