package data_integration

import (
	"context"
	"database/sql"
	"fennel/engine/ast"
	"fennel/lib/data_integration"
	"fennel/tier"
	"fmt"
)

type connectorSer struct {
	Name        string  `db:"name"`
	SourceName  string  `db:"source_name"`
	SourceType  string  `db:"source_type"`
	StreamName  string  `db:"stream_name"`
	Version     int     `db:"version"`
	Destination string  `db:"destination"`
	QuerySer    []byte  `db:"query_ser"`
	LastUpdated []uint8 `db:"last_updated"`
	ConnId      string  `db:"conn_id"`
	CursorField string  `db:"cursor_field"`
	Active      bool    `db:"active"`
}

func (ser connectorSer) ToConnector() (data_integration.Connector, error) {
	var conn data_integration.Connector
	conn.Name = ser.Name
	if err := ast.Unmarshal(ser.QuerySer, &conn.Query); err != nil {
		return data_integration.Connector{}, err
	}
	conn.Active = ser.Active
	conn.SourceName = ser.SourceName
	conn.SourceType = ser.SourceType
	conn.Version = ser.Version
	conn.Destination = ser.Destination
	conn.StreamName = ser.StreamName
	conn.ConnId = ser.ConnId
	conn.CursorField = ser.CursorField
	return conn, nil
}

func Store(ctx context.Context, tier tier.Tier, conn data_integration.Connector, connId string) error {
	querySer, err := ast.Marshal(conn.Query)
	if err != nil {
		return fmt.Errorf("failed to marshal query: %w", err)
	}
	if len(conn.Name) > 255 {
		return fmt.Errorf("data_integration name can not be longer than 255 chars")
	}
	// Check if source exists
	var srcSer sourceSer
	err = tier.DB.GetContext(ctx, &srcSer, "SELECT * FROM source WHERE name = ?", conn.SourceName)
	if err == sql.ErrNoRows {
		return fmt.Errorf("source %s for the connector does not exist", conn.SourceName)
	} else if err != nil {
		return fmt.Errorf("failed to check if source exists: %w", err)
	}

	sql := `INSERT INTO connector (name, version, source_name, source_type, destination, query_ser, conn_id, cursor_field, stream_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err = tier.DB.QueryContext(ctx, sql, conn.Name, conn.Version, conn.SourceName, conn.SourceType, conn.Destination, querySer, connId, conn.CursorField, conn.StreamName)
	return err
}

func Retrieve(ctx context.Context, tier tier.Tier, name string) (data_integration.Connector, error) {
	var conn connectorSer
	err := tier.DB.GetContext(ctx, &conn, `SELECT * FROM connector WHERE name = ?`, name)
	if err != nil && err == sql.ErrNoRows {
		return data_integration.Connector{}, data_integration.ErrConnNotFound
	} else if err != nil {
		return data_integration.Connector{}, err
	}
	return conn.ToConnector()
}

func RetrieveActive(ctx context.Context, tier tier.Tier) ([]data_integration.Connector, error) {
	var conn []connectorSer
	err := tier.DB.SelectContext(ctx, &conn, `SELECT * FROM connector WHERE active = True`)
	if err != nil {
		return nil, err
	}
	ret := make([]data_integration.Connector, len(conn))
	for i := range conn {
		ret[i], err = conn[i].ToConnector()
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

// Check if any connector is using this source.
func CheckIfInUse(ctx context.Context, tier tier.Tier, sourceName string) error {
	var conn []connectorSer
	err := tier.DB.SelectContext(ctx, &conn, `SELECT * FROM connector WHERE source_name = ?`, sourceName)
	if err != nil {
		return err
	}
	if len(conn) > 0 {
		return fmt.Errorf("source %s is in use by %d connectors", sourceName, len(conn))
	}
	return nil
}

func Activate(ctx context.Context, tier tier.Tier, name string) error {
	_, err := tier.DB.ExecContext(ctx, `UPDATE connector SET active = TRUE WHERE name = ?`, name)
	return err
}

func Disable(ctx context.Context, tier tier.Tier, name string) error {
	_, err := tier.DB.ExecContext(ctx, `UPDATE connector SET active = FALSE WHERE name = ?`, name)
	return err
}

func Delete(ctx context.Context, tier tier.Tier, name string) error {
	_, err := tier.DB.ExecContext(ctx, `DELETE FROM connector WHERE name = ?`, name)
	return err
}

func Update(ctx context.Context, tier tier.Tier, conn data_integration.Connector) error {
	querySer, err := ast.Marshal(conn.Query)
	if err != nil {
		return fmt.Errorf("failed to marshal query: %w", err)
	}
	_, err = tier.DB.ExecContext(ctx, `UPDATE connector  SET version = ?, query_ser = ?, active = TRUE WHERE name = ?`, conn.Version, querySer, conn.Name)
	return err
}
