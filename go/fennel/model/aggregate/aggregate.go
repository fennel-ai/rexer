package aggregate

import (
	"database/sql"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/tier"
	"fmt"
)

func Store(plane tier.Tier, aggtype ftypes.AggType, name ftypes.AggName, querySer []byte, ts ftypes.Timestamp, optionSer []byte) error {
	if len(aggtype) > 255 {
		return fmt.Errorf("aggregate type can not be longer than 255 chars")
	}
	if len(name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	sql := `INSERT INTO aggregate_config VALUES (?, ?, ?, ?, ?, ?)`
	_, err := plane.DB.Query(sql, plane.CustID, aggtype, name, querySer, ts, optionSer)
	return err
}

func Retrieve(plane tier.Tier, aggregateType ftypes.AggType, name ftypes.AggName) (aggregate.AggregateSer, error) {
	var ret aggregate.AggregateSer
	err := plane.DB.Get(&ret, `
			SELECT * FROM aggregate_config 
			WHERE cust_id = ? 
			  AND aggregate_type = ? 
			  AND name = ?`,
		plane.CustID, aggregateType, name,
	)
	if err != nil && err == sql.ErrNoRows {
		return aggregate.AggregateSer{}, aggregate.ErrNotFound
	} else if err != nil {
		return aggregate.AggregateSer{}, err
	}
	return ret, nil
}

func RetrieveAll(plane tier.Tier, aggtype ftypes.AggType) ([]aggregate.AggregateSer, error) {
	ret := make([]aggregate.AggregateSer, 0)
	err := plane.DB.Select(&ret, `
			SELECT * FROM aggregate_config 
			WHERE cust_id = ? 
			  AND aggregate_type = ? 
			  `,
		plane.CustID, aggtype,
	)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
