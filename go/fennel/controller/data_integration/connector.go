package data_integration

import (
	"context"
	"errors"
	"fennel/lib/data_integration"
	connectorModel "fennel/model/data_integration"

	"fennel/tier"
	"fmt"
)

func StoreConnector(ctx context.Context, tier tier.Tier, conn data_integration.Connector) error {
	if err := conn.Validate(); err != nil {
		return err
	}

	conn2, err := connectorModel.Retrieve(ctx, tier, conn.Name)
	if err != nil {
		if errors.Is(err, data_integration.ErrConnNotFound) {
			tier.Logger.Debug("Storing new connector")
			// Write the connector to Airbyte
			if tier.AirbyteClient.IsAbsent() {
				return fmt.Errorf("error: Airbyte client is not initialized")
			}
			source, err := connectorModel.RetrieveSource(ctx, tier, conn.SourceName)
			if err != nil {
				return fmt.Errorf("error: failed to retrieve source: %w", err)
			}
			connId, err := tier.AirbyteClient.MustGet().CreateConnector(source, conn)
			if err != nil {
				return fmt.Errorf("error: failed to create connector: %w", err)
			}
			// Finally, write the connector to the db
			return connectorModel.Store(ctx, tier, conn, connId)
		} else {
			return fmt.Errorf("failed to retrieve connector: %w", err)
		}
	} else {
		if conn.Equals(conn2) {
			if !conn2.Active {
				err := connectorModel.Activate(ctx, tier, conn.Name)
				if err != nil {
					return fmt.Errorf("failed to reactivate connector '%s': %w", conn.Name, err)
				}
			}
			return nil
		} else {
			return fmt.Errorf("connector already present but with different params")
		}
	}

}

func DeactivateConnector(ctx context.Context, tier tier.Tier, name string) error {
	conn, err := connectorModel.Retrieve(ctx, tier, name)
	if err != nil {
		return fmt.Errorf("failed to retrieve connector: %w", err)
	}
	if !conn.Active {
		return nil
	}
	return connectorModel.Deactivate(ctx, tier, name)
}
