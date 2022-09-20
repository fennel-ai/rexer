package aggregate

import (
	"context"
	"database/sql"
	"fmt"

	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/tier"

	"google.golang.org/protobuf/proto"
)

func Store(ctx context.Context, tier tier.Tier, agg aggregate.Aggregate) error {
	var querySer []byte
	var err error
	if agg.Mode == aggregate.RQL {
		querySer, err = ast.Marshal(agg.Query)
		if err != nil {
			return fmt.Errorf("failed to marshal query: %w", err)
		}
	} else {
		querySer = agg.PythonQuery
	}

	optionSer, err := proto.Marshal(aggregate.ToProtoOptions(agg.Options))
	if err != nil {
		return fmt.Errorf("failed to marshal options: %w", err)
	}
	if len(agg.Name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	sql := `INSERT INTO aggregate_config (name, query_ser, timestamp, source, mode, options_ser) VALUES (?, ?, ?, ?, ?, ?)`
	_, err = tier.DB.QueryContext(ctx, sql, agg.Name, querySer, agg.Timestamp, agg.Source, agg.Mode, optionSer)
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

func RetrieveAll(ctx context.Context, tier tier.Tier) ([]aggregate.Aggregate, error) {
	var aggregates []aggregate.AggregateSer
	err := tier.DB.SelectContext(ctx, &aggregates, `SELECT * FROM aggregate_config`)
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

func Retrieve(ctx context.Context, tier tier.Tier, name ftypes.AggName) (aggregate.Aggregate, error) {
	var agg aggregate.AggregateSer
	err := tier.DB.GetContext(ctx, &agg, `SELECT * FROM aggregate_config WHERE name = ?`, name)
	if err != nil && err == sql.ErrNoRows {
		return aggregate.Aggregate{}, aggregate.ErrNotFound
	} else if err != nil {
		return aggregate.Aggregate{}, err
	}
	return agg.ToAggregate()
}

func Deactivate(ctx context.Context, tier tier.Tier, name ftypes.AggName) error {
	_, err := tier.DB.ExecContext(ctx, `UPDATE aggregate_config SET active = FALSE WHERE name = ?`, name)
	return err
}

func Activate(ctx context.Context, tier tier.Tier, name ftypes.AggName) error {
	_, err := tier.DB.ExecContext(ctx, `UPDATE aggregate_config SET active = TRUE WHERE name = ?`, name)
	return err
}
