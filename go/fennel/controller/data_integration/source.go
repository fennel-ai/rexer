package data_integration

import (
	"context"
	"encoding/json"
	"errors"
	"fennel/lib/data_integration"
	diModel "fennel/model/data_integration"
	"fennel/tier"
	"fmt"
)

func UnmarshalSource(data []byte) (data_integration.Source, error) {
	var srcInfo map[string]interface{}
	err := json.Unmarshal(data, &srcInfo)
	if err != nil {
		return nil, err
	}
	switch srcInfo["type"] {
	case "S3":
		src := data_integration.S3{}
		err = json.Unmarshal(data, &src)
		return src, err
	case "BigQuery":
		src := data_integration.BigQuery{}
		err = json.Unmarshal(data, &src)
		return src, err
	case "Postgres":
		src := data_integration.Postgres{}
		err = json.Unmarshal(data, &src)
		return src, err
	case "MySQL":
		src := data_integration.MySQL{}
		err = json.Unmarshal(data, &src)
		return src, err
	case "Snowflake":
		src := data_integration.Snowflake{}
		err = json.Unmarshal(data, &src)
		return src, err
	default:
		return nil, fmt.Errorf("unknown source type: %s", srcInfo["type"])
	}
}

func StoreSource(ctx context.Context, tier tier.Tier, src data_integration.Source) error {
	if err := src.Validate(); err != nil {
		return err
	}
	// Check if source already exists in db
	src2, err := diModel.RetrieveSource(ctx, tier, src.GetSourceName())
	if err != nil {
		if errors.Is(err, data_integration.ErrSrcNotFound) {
			tier.Logger.Debug("Storing new src " + src.GetSourceName())

			// Write the source to Airbyte
			if tier.AirbyteClient.IsAbsent() {
				return fmt.Errorf("error: Airbyte client is not initialized")
			}
			srcId, err := tier.AirbyteClient.MustGet().CreateSource(src)
			if err != nil {
				return fmt.Errorf("error: failed to create source: %w", err)
			}

			// Write the source to the db
			return diModel.StoreSource(ctx, tier, src, srcId)
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
	if err := diModel.CheckIfInUse(ctx, tier, name); err != nil {
		return err
	}
	src, err := diModel.RetrieveSource(ctx, tier, name)
	if err != nil {
		return fmt.Errorf("failed to retrieve source: %w", err)
	}
	if tier.AirbyteClient.IsAbsent() {
		return fmt.Errorf("error: Airbyte client is not initialized")
	}
	if err = tier.AirbyteClient.MustGet().DeleteSource(src); err != nil {
		return fmt.Errorf("error: failed to delete source: %w", err)
	}
	return diModel.DeleteSource(ctx, tier, src)
}
