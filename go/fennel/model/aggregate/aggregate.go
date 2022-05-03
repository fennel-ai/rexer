package aggregate

import (
	"context"
	"database/sql"
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/tier"
)

func Store(ctx context.Context, tier tier.Tier, name ftypes.AggName, querySer []byte, ts ftypes.Timestamp, optionSer []byte, servingDataSer []byte) error {
	if len(name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	sql := `INSERT INTO aggregate_config (name, query_ser, timestamp, options_ser, serving_data_ser) VALUES (?, ?, ?, ?, ?)`
	_, err := tier.DB.QueryContext(ctx, sql, name, querySer, ts, optionSer, servingDataSer)
	return err
}

func Retrieve(ctx context.Context, tier tier.Tier, name ftypes.AggName) (aggregate.AggregateSer, error) {
	var ret aggregate.AggregateSer
	err := tier.DB.GetContext(ctx, &ret, `SELECT * FROM aggregate_config WHERE name = ? AND active = TRUE`, name)
	if err != nil && err == sql.ErrNoRows {
		return aggregate.AggregateSer{}, aggregate.ErrNotFound
	} else if err != nil {
		return aggregate.AggregateSer{}, err
	}
	return ret, nil
}

func RetrieveActive(ctx context.Context, tier tier.Tier) ([]aggregate.AggregateSer, error) {
	ret := make([]aggregate.AggregateSer, 0)
	err := tier.DB.SelectContext(ctx, &ret, `SELECT * FROM aggregate_config WHERE active = TRUE`)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func RetrieveAll(ctx context.Context, tier tier.Tier) ([]aggregate.AggregateSer, error) {
	ret := make([]aggregate.AggregateSer, 0)
	err := tier.DB.SelectContext(ctx, &ret, `SELECT * FROM aggregate_config`)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func RetrieveNoFilter(ctx context.Context, tier tier.Tier, name ftypes.AggName) (aggregate.AggregateSer, error) {
	var ret aggregate.AggregateSer
	err := tier.DB.GetContext(ctx, &ret, `SELECT * FROM aggregate_config WHERE name = ?`, name)
	if err != nil && err == sql.ErrNoRows {
		return aggregate.AggregateSer{}, aggregate.ErrNotFound
	} else if err != nil {
		return aggregate.AggregateSer{}, err
	}
	return ret, nil
}

func Deactivate(ctx context.Context, tier tier.Tier, name ftypes.AggName) error {
	_, err := tier.DB.ExecContext(ctx, `UPDATE aggregate_config SET active = FALSE WHERE name = ?`, name)
	return err
}
