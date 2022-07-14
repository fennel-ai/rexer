package connector

import (
	"context"
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/connector"
	"fennel/lib/ftypes"
	"fennel/tier"
	"fmt"
	"google.golang.org/protobuf/proto"
)

type connectorSer struct {
	Name        string `db:"name"`
	SourceName  string `db:"source_name"`
	SourceType  string `db:"source_type"`
	Version     string `db:"version"`
	Destination string `db:"destination"`
	QuerySer    []byte `db:"options_ser"`
	Config      []byte `db:"config"`
	Active      bool   `db:"active"`
}

func (ser connectorSer) ToConnector() (connector.Connector, error) {
	var conn connector.Connector
	conn.Name = ser.Name
	if err := ast.Unmarshal(ser.QuerySer, &conn.Query); err != nil {
		return connector.Connector{}, err
	}
	var config connector.ConnectorConfig
	if err := proto.Unmarshal(ser.Config, &config); err != nil {
		return connector.Connector{}, err
	}
	conn.Config = connector.FromProtoConfig(&config)
	conn.Active = ser.Active
	conn.SourceName = ser.SourceName
	conn.SourceType = ser.SourceType
	conn.Version = ser.Version
	conn.Destination = ser.Destination
	return conn, nil
}

func Store(ctx context.Context, tier tier.Tier, conn connector.Connector) error {
	querySer, err := ast.Marshal(conn.Query)
	if err != nil {
		return fmt.Errorf("failed to marshal query: %w", err)
	}
	configSer, err := proto.Marshal(connector.ToProtoConfig(conn.Config))
	if err != nil {
		return fmt.Errorf("failed to marshal options: %w", err)
	}
	if len(conn.Name) > 255 {
		return fmt.Errorf("connector name can not be longer than 255 chars")
	}
	sql := `INSERT INTO connector (name, query_ser, timestamp, source, options_ser) VALUES (?, ?, ?, ?, ?)`
	_, err = tier.DB.QueryContext(ctx, sql, conn.Name, conn.Version, conn.SourceName, conn.SourceType, conn.Destination, querySer, configSer)
	return err
}

func RetrieveActive(ctx context.Context, tier tier.Tier) ([]aggregate.Aggregate, error) {
	var aggregates []aggregate.AggregateSer
	err := tier.DB.SelectContext(ctx, &aggregates, `SELECT * FROM aggregate_config WHERE active = TRUE`)
	if err != nil {
		return nil, err
	}
	ret := make([]aggregate.Aggregate, len(aggregates))
	for i := range aggregates {
		ret[i], err = aggregates[i].ToAggregate()
		if err != nil {
			return nil, fmt.Errorf("failed to convert %v to aggregate: %w", aggregates[i], err)
		}
	}
	return ret, nil
}

func Deactivate(ctx context.Context, tier tier.Tier, name ftypes.AggName) error {
	_, err := tier.DB.ExecContext(ctx, `UPDATE aggregate_config SET active = FALSE WHERE name = ?`, name)
	return err
}
