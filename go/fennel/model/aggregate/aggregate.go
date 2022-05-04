package aggregate

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/tier"
)

func Store(ctx context.Context, tier tier.Tier, name ftypes.AggName, querySer []byte, ts ftypes.Timestamp, optionSer []byte) error {
	if len(name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	sql := `INSERT INTO aggregate_config (name, query_ser, timestamp, options_ser) VALUES (?, ?, ?, ?)`
	_, err := tier.DB.QueryContext(ctx, sql, name, querySer, ts, optionSer)
	return err
}

func InitializeAggUpdateVersion(ctx context.Context, tier tier.Tier, name ftypes.AggName, duration uint64) error {
	if len(name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	_, err := tier.DB.ExecContext(ctx, `INSERT INTO aggregate_serving_data (name, duration, update_version) VALUES (?, ?, ?)`, name, duration, 0)
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

func GetLatestUpdatedVersion(ctx context.Context, tier tier.Tier, name ftypes.AggName, duration uint64) (uint64, error) {
	var value [][]byte = nil
	err := tier.DB.SelectContext(ctx, &value, `SELECT update_version FROM aggregate_serving_data WHERE name = ? AND duration = ? LIMIT 1`, name, duration)
	if err != nil {
		return 0, err
	} else if len(value) == 0 {
		return 0, aggregate.ErrNotFound
	}
	return strconv.ParseUint(string(value[0]), 10, 64)
}

func UpdateAggregateVersion(ctx context.Context, tier tier.Tier, name ftypes.AggName, duration uint64, update_version uint64) error {
	_, err := tier.DB.ExecContext(ctx, `UPDATE aggregate_serving_data SET update_version = ? WHERE name = ? AND duration = ?`, update_version, name, duration)
	return err
}
