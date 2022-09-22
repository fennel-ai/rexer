package feature

import (
	"context"
	"fennel/lib/feature"
	modelFeature "fennel/model/feature"
	"fennel/tier"
	"fmt"
)

func Store(ctx context.Context, tr tier.Tier, f feature.Feature) error {
	if err := f.Validate(); err != nil {
		return fmt.Errorf("invalid feature: %v", err)
	}
	// Check if agg already exists in db
	f2, err := modelFeature.RetrieveLatest(ctx, tr, f.FeatureName)
	if err != nil {
		if err == modelFeature.ErrNotFound {
			err := modelFeature.Store(ctx, tr, f)
			if err != nil {
				return fmt.Errorf("failed to store feature: %v", err)
			}
		} else {
			return fmt.Errorf("error retrieving feature: %v", err)
		}
	} else {
		// Check if feature stored and feature to be stored are the same
		if f2.Version > f.Version {
			return fmt.Errorf("feature already exists with higher version %v", f2.Version)
		}
		if f.Version == f2.Version {
			if string(f2.Query) == string(f.Query) {
				return nil
			} else {
				return fmt.Errorf("feature already already present but with different query")
			}
		}
		err := modelFeature.Store(ctx, tr, f)
		if err != nil {
			return fmt.Errorf("error storing feature: %v", err)
		}
	}
	return nil
}

func RetrieveLatest(ctx context.Context, tr tier.Tier, featureName string) (feature.Feature, error) {
	return modelFeature.RetrieveLatest(ctx, tr, featureName)
}
