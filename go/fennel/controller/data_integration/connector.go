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
	AIRBYTE_DATA_FIELD           = "_airbyte_data"
	AIRBYTE_STREAM_NAME          = "_airbyte_stream"
	AIRBYTE_CONNECTOR_NAME_FIELD = "stream_name"
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
		if err = conn.Equals(conn2); err == nil {
			if !conn2.Active {
				err := connectorModel.Activate(ctx, tier, conn.Name)
				if err != nil {
					return fmt.Errorf("failed to reactivate connector '%s': %w", conn.Name, err)
				}
			}
			return nil
		} else {
			return fmt.Errorf("connector already present but with different params: %w", err)
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

func ReadBatch(ctx context.Context, consumer kafka.FConsumer, streamName string, count int, timeout time.Duration) ([]value.Value, [][16]byte, error) {
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
			d.Set(AIRBYTE_CONNECTOR_NAME_FIELD, dict.GetUnsafe(AIRBYTE_STREAM_NAME))
			streams = append(streams, d)
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
