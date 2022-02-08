package aggregate

import (
	"database/sql"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/tier"
	"fmt"
)

func Store(tier tier.Tier, name ftypes.AggName, querySer []byte, ts ftypes.Timestamp, optionSer []byte) error {
	if len(name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	sql := `INSERT INTO aggregate_config (name, query_ser, timestamp, options_ser) VALUES (?, ?, ?, ?)`
	_, err := tier.DB.Query(sql, name, querySer, ts, optionSer)
	return err
}

func Retrieve(tier tier.Tier, name ftypes.AggName) (aggregate.AggregateSer, error) {
	var ret aggregate.AggregateSer
	err := tier.DB.Get(&ret, `SELECT name, query_ser, timestamp, options_ser FROM aggregate_config WHERE name = ? AND active = TRUE`, name)
	if err != nil && err == sql.ErrNoRows {
		return aggregate.AggregateSer{}, aggregate.ErrNotFound
	} else if err != nil {
		return aggregate.AggregateSer{}, err
	}
	return ret, nil
}

func RetrieveAll(tier tier.Tier) ([]aggregate.AggregateSer, error) {
	ret := make([]aggregate.AggregateSer, 0)
	err := tier.DB.Select(&ret, `SELECT name, query_ser, timestamp, options_ser FROM aggregate_config WHERE active = TRUE`)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func Deactivate(tier tier.Tier, name ftypes.AggName) error {
	_, err := tier.DB.Exec(`UPDATE aggregate_config SET active = FALSE WHERE name = ?`, name)
	return err
}
