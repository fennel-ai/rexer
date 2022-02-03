package aggregate

import (
	"database/sql"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/plane"
	"fmt"
)

func Store(instance plane.Plane, aggtype ftypes.AggType, name ftypes.AggName, querySer []byte, ts ftypes.Timestamp, optionSer []byte) error {
	if len(aggtype) > 255 {
		return fmt.Errorf("aggregate type can not be longer than 255 chars")
	}
	if len(name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	sql := `INSERT INTO aggregate_config VALUES (?, ?, ?, ?, ?, ?)`
	_, err := instance.DB.Query(sql, instance.CustID, aggtype, name, querySer, ts, optionSer)
	return err
}

func Retrieve(instance plane.Plane, aggregateType ftypes.AggType, name ftypes.AggName) (aggregate.AggregateSer, error) {
	var ret aggregate.AggregateSer
	err := instance.DB.Get(&ret, `
			SELECT * FROM aggregate_config 
			WHERE cust_id = ? 
			  AND aggregate_type = ? 
			  AND name = ?`, instance.CustID, aggregateType, name,
	)
	if err != nil && err == sql.ErrNoRows {
		return aggregate.AggregateSer{}, aggregate.ErrNotFound
	} else if err != nil {
		return aggregate.AggregateSer{}, err
	}
	return ret, nil
}

func RetrieveAll(instance plane.Plane, aggtype ftypes.AggType) ([]aggregate.AggregateSer, error) {
	ret := make([]aggregate.AggregateSer, 0)
	err := instance.DB.Select(&ret, `
			SELECT * FROM aggregate_config 
			WHERE cust_id = ? 
			  AND aggregate_type = ? 
			  `, instance.CustID, aggtype,
	)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
