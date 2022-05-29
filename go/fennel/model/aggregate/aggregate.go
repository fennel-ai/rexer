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

type AggregateSer struct {
	Name      ftypes.AggName   `db:"name"`
	QuerySer  []byte           `db:"query_ser"`
	Timestamp ftypes.Timestamp `db:"timestamp"`
	OptionSer []byte           `db:"options_ser"`
	Active    bool             `db:"active"`
	Id        ftypes.AggId     `db:"id"`
}

func (ser AggregateSer) ToAggregate() (aggregate.Aggregate, error) {
	var agg aggregate.Aggregate
	agg.Timestamp = ser.Timestamp
	agg.Name = ser.Name
	if err := ast.Unmarshal(ser.QuerySer, &agg.Query); err != nil {
		return aggregate.Aggregate{}, err
	}
	var popt aggregate.AggOptions
	if err := proto.Unmarshal(ser.OptionSer, &popt); err != nil {
		return aggregate.Aggregate{}, err
	}
	agg.Options = aggregate.FromProtoOptions(&popt)
	agg.Active = ser.Active
	agg.Id = ser.Id
	return agg, nil
}

func Store(ctx context.Context, tier tier.Tier, agg aggregate.Aggregate) error {
	querySer, err := ast.Marshal(agg.Query)
	if err != nil {
		return fmt.Errorf("failed to marshal query: %w", err)
	}
	optionSer, err := proto.Marshal(aggregate.ToProtoOptions(agg.Options))
	if err != nil {
		return fmt.Errorf("failed to marshal options: %w", err)
	}
	if len(agg.Name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	sql := `INSERT INTO aggregate_config (name, query_ser, timestamp, options_ser) VALUES (?, ?, ?, ?)`
	_, err = tier.DB.QueryContext(ctx, sql, agg.Name, querySer, agg.Timestamp, optionSer)
	return err
}

func RetrieveActive(ctx context.Context, tier tier.Tier) ([]aggregate.Aggregate, error) {
	var aggregates []AggregateSer
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
	var aggregates []AggregateSer
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
	var agg AggregateSer
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
