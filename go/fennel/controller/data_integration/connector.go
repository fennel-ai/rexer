package data_integration

import (
	"context"
	"errors"
	"fennel/kafka"
	"fennel/lib/data_integration"
	"fennel/lib/value"
	connectorModel "fennel/model/data_integration"
	"github.com/zeebo/xxh3"
	"time"

	"fennel/tier"
	"fmt"
)

const (
	AIRBYTE_DATA_FIELD             = "_airbyte_data"
	AIRBYTE_STREAM_NAME            = "_airbyte_stream"
	AIRBYTE_CONNECTOR_STREAM_FIELD = "stream_name"
	AIRBYTE_CONNECTOR_NAME_FIELD   = "connector_name"
)

func StoreConnector(ctx context.Context, tier tier.Tier, conn data_integration.Connector) error {
	if err := conn.Validate(); err != nil {
		return err
	}

	if tier.AirbyteClient.IsAbsent() {
		return fmt.Errorf("error: Airbyte client is not initialized")
	}

	source, err := connectorModel.RetrieveSource(ctx, tier, conn.SourceName)
	if err != nil {
		return fmt.Errorf("error: failed to retrieve source: %w", err)
	}
	conn2, err := connectorModel.Retrieve(ctx, tier, conn.Name)

	if err != nil {
		if errors.Is(err, data_integration.ErrConnNotFound) {
			tier.Logger.Debug("Storing new connector: " + conn.Name)
			// Write the connector to Airbyte
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
		if err = conn.Equals(conn2); err == nil {
			if !conn2.Active {
				if err = connectorModel.Activate(ctx, tier, conn.Name); err != nil {
					return fmt.Errorf("failed to reactivate connector '%s': %w", conn.Name, err)
				}
			}
			return nil
		} else {
			// Update the connector in Airbyte
			if conn.Version > conn2.Version {
				conn.ConnId = conn2.ConnId
				tier.Logger.Debug("Updating connector: " + conn.Name)
				tier.Logger.Debug("Disabling active connector: " + conn.Name)
				if err = tier.AirbyteClient.MustGet().DisableConnector(source, conn); err != nil {
					return fmt.Errorf("error: failed to disable connector: %w", err)
				}
				if err = connectorModel.Disable(ctx, tier, conn.Name); err != nil {
					return fmt.Errorf("failed to disable connector '%s': %w", conn.Name, err)
				}
				fmt.Println(conn)
				fmt.Println(conn2)
				tier.Logger.Debug("Updating and enabling connector: " + conn.Name)
				if err = tier.AirbyteClient.MustGet().UpdateConnector(source, conn); err != nil {
					return fmt.Errorf("error: failed to update connector: %w", err)
				}
				// Finally, set state of connector to active
				if err = connectorModel.Update(ctx, tier, conn); err != nil {
					return fmt.Errorf("failed to reactivate connector '%s': %w", conn.Name, err)
				}
				return nil
			} else if conn.Version < conn2.Version {
				return fmt.Errorf("error: connector '%s' has been updated to version %d since you last retrieved it", conn.Name, conn2.Version)
			}
			return fmt.Errorf("connector already present but with different params: %w", err)
		}
	}
}

func DisableConnector(ctx context.Context, tier tier.Tier, name string) error {
	conn, err := connectorModel.Retrieve(ctx, tier, name)
	if err != nil {
		return fmt.Errorf("failed to retrieve connector: %w", err)
	}
	if !conn.Active {
		return nil
	}
	tier.Logger.Debug("Disabling active connector: " + conn.Name)
	if tier.AirbyteClient.IsAbsent() {
		return fmt.Errorf("error: Airbyte client is not initialized")
	}
	source, err := connectorModel.RetrieveSource(ctx, tier, conn.SourceName)
	if err != nil {
		return fmt.Errorf("error: failed to retrieve source: %w", err)
	}
	if err = tier.AirbyteClient.MustGet().DisableConnector(source, conn); err != nil {
		return fmt.Errorf("error: failed to disable connector: %w", err)
	}
	// Finally, write the connector to the db
	return connectorModel.Disable(ctx, tier, conn.Name)
}

func DeleteConnector(ctx context.Context, tier tier.Tier, name string) error {
	conn, err := connectorModel.Retrieve(ctx, tier, name)
	if err != nil {
		return fmt.Errorf("failed to retrieve connector: %w", err)
	}
	if tier.AirbyteClient.IsAbsent() {
		return fmt.Errorf("error: Airbyte client is not initialized")
	}
	if err = tier.AirbyteClient.MustGet().DeleteConnector(conn); err != nil {
		return fmt.Errorf("error: failed to delete connector: %w", err)
	}
	return connectorModel.Delete(ctx, tier, name)
}

func ReadBatch(ctx context.Context, consumer kafka.FConsumer, streamName, connName string, count int, timeout time.Duration) ([]value.Value, [][16]byte, error) {
	msgs, err := consumer.ReadBatch(ctx, count, timeout)
	if err != nil {
		return nil, nil, err
	}
	streams := make([]value.Value, 0)
	hashes := make([][16]byte, 0)
	for _, msg := range msgs {
		val, err := value.FromJSON(msg)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse message: %w", err)
		}
		if dict, ok := val.(value.Dict); ok {
			if string(dict.GetUnsafe(AIRBYTE_STREAM_NAME).(value.String)) != streamName {
				continue
			}
			d := dict.GetUnsafe(AIRBYTE_DATA_FIELD).(value.Dict)
			d.Set(AIRBYTE_CONNECTOR_STREAM_FIELD, dict.GetUnsafe(AIRBYTE_STREAM_NAME))
			streams = append(streams, d)
			// This field is added only so that the same stream can be used for multiple connectors and is not deduped by redis
			d.Set(AIRBYTE_CONNECTOR_NAME_FIELD, value.String(connName))
			serialized, err := d.Marshal()
			if err != nil {
				return nil, nil, fmt.Errorf("failed to serialize message: %w", err)
			}
			hashes = append(hashes, xxh3.Hash128(serialized).Bytes())
		} else {
			return nil, nil, fmt.Errorf("message is not a dict")
		}
	}

	return streams, hashes, nil
}
