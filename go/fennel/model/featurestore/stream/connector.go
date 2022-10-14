package stream

import (
	"context"
	"database/sql"
	"fmt"

	"fennel/featurestore/tier"
	lib "fennel/lib/featurestore/stream"
)

func StoreConnector(ctx context.Context, tier tier.Tier, conn lib.Connector, connId string) error {
	// Check if source exists
	var srcSer sourceSer
	err := tier.DB.GetContext(ctx, &srcSer, "SELECT * FROM source WHERE name = ?", conn.SourceName)
	if err == sql.ErrNoRows {
		return fmt.Errorf("source %s for the connector does not exist", conn.SourceName)
	} else if err != nil {
		return fmt.Errorf("failed to check if source exists: %w", err)
	}

	sql := `INSERT INTO connector (name, source_name, source_type, stream_name, conn_id, function, cursor_field, table_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err = tier.DB.QueryContext(ctx, sql, conn.Name, conn.SourceName, conn.SourceType, conn.StreamName, connId, conn.Function, conn.CursorField, conn.TableName)
	return err
}

func RetrieveConnector(ctx context.Context, tier tier.Tier, name string) (lib.Connector, error) {
	var conn lib.Connector
	err := tier.DB.GetContext(ctx, &conn, `SELECT * FROM connector WHERE name = ?`, name)
	if err != nil && err == sql.ErrNoRows {
		return lib.Connector{}, lib.ErrConnNotFound
	} else if err != nil {
		return lib.Connector{}, err
	}
	return conn, nil
}

// Check if any connector is using this source.
func CheckIfInUse(ctx context.Context, tier tier.Tier, sourceName string) error {
	var conns []lib.Connector
	err := tier.DB.SelectContext(ctx, &conns, `SELECT * FROM connector WHERE source_name = ?`, sourceName)
	if err != nil {
		return err
	}
	if len(conns) > 0 {
		return fmt.Errorf("source %s is in use by %d connectors", sourceName, len(conns))
	}
	return nil
}

func DeleteConnector(ctx context.Context, tier tier.Tier, name string) error {
	_, err := tier.DB.ExecContext(ctx, `DELETE FROM connector WHERE name = ?`, name)
	return err
}
