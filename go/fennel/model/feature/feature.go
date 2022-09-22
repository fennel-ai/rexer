package feature

import (
	"context"
	"errors"
	"fennel/lib/feature"
	"fennel/tier"
)

var ErrNotFound = errors.New("feature not found")

func Store(ctx context.Context, tier tier.Tier, f feature.Feature) error {
	sql := `INSERT INTO feature_registry (feature_name, query, version) VALUES (?, ?, ?)`
	_, err := tier.DB.QueryContext(ctx, sql, f.FeatureName, f.Query, f.Version)
	return err
}

func RetrieveLatest(ctx context.Context, tier tier.Tier, featureName string) (feature.Feature, error) {
	var f feature.Feature
	err := tier.DB.GetContext(ctx, &f, `SELECT * FROM feature_registry WHERE feature_name = ? ORDER BY version DESC LIMIT 1`, featureName)
	if err != nil {
		return feature.Feature{}, ErrNotFound
	}
	return f, nil
}

func Retrieve(ctx context.Context, tier tier.Tier, featureName string, version int) (feature.Feature, error) {
	var f feature.Feature
	err := tier.DB.GetContext(ctx, &f, `SELECT * FROM feature_registry WHERE feature_name = ? AND version = ?`, featureName, version)
	return f, err
}
