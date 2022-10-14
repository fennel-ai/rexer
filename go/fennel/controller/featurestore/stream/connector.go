package stream

import (
	"context"
	"errors"
	"fmt"

	"fennel/featurestore/tier"
	lib "fennel/lib/featurestore/stream"
	"fennel/model/featurestore/stream"
)

func StoreConnector(ctx context.Context, tier tier.Tier, conn lib.Connector) error {
	if err := conn.Validate(); err != nil {
		return err
	}

	if tier.AirbyteClient.IsAbsent() {
		return fmt.Errorf("error: Airbyte client is not initialized")
	}

	src, err := stream.RetrieveSource(ctx, tier, conn.SourceName)
	if err != nil {
		return fmt.Errorf("error: failed to retrieve source: %w", err)
	}
	conn2, err := stream.RetrieveConnector(ctx, tier, conn.Name)

	if err != nil {
		if errors.Is(err, lib.ErrConnNotFound) {
			tier.Logger.Debug("Storing new connector: " + conn.Name)
			// Write the connector to Airbyte
			diSrc, err := toDataIntegrationSource(src)
			if err != nil {
				return fmt.Errorf("error: failed to convert stream.Source to data_integration.Source: %w", err)
			}
			connId, err := tier.AirbyteClient.MustGet().CreateConnector(diSrc, toDataIntegrationConnector(conn))
			if err != nil {
				return fmt.Errorf("error: failed to create connector: %w", err)
			}
			// Finally, write the connector to the db
			return stream.StoreConnector(ctx, tier, conn, connId)
		} else {
			return fmt.Errorf("failed to retrieve connector: %w", err)
		}
	}

	err = conn.Equals(conn2)
	if err != nil {
		return fmt.Errorf("connector already present but with different params : %w", err)
	}
	return nil
}

func DeleteConnector(ctx context.Context, tier tier.Tier, name string) error {
	conn, err := stream.RetrieveConnector(ctx, tier, name)
	if err != nil {
		return fmt.Errorf("failed to retrieve connector: %w", err)
	}
	if tier.AirbyteClient.IsAbsent() {
		return fmt.Errorf("error: Airbyte client is not initialized")
	}
	if err = tier.AirbyteClient.MustGet().DeleteConnector(toDataIntegrationConnector(conn)); err != nil {
		return fmt.Errorf("error: failed to delete connector: %w", err)
	}
	return stream.DeleteConnector(ctx, tier, name)
}
