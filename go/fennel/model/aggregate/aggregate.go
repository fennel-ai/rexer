package aggregate

import (
	"database/sql"
	"fennel/instance"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fmt"
)

type notFound int

func (_ notFound) Error() string {
	return "aggregate not found"
}

var ErrNotFound = notFound(1)
var _ error = ErrNotFound

func Store(instance instance.Instance, aggtype ftypes.AggType, name ftypes.AggName, querySer []byte, ts ftypes.Timestamp, optionSer []byte) error {
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

func Retrieve(instance instance.Instance, aggregateType ftypes.AggType, name ftypes.AggName) (aggregate.AggregateSer, error) {
	var ret aggregate.AggregateSer
	err := instance.DB.Get(&ret, `
			SELECT * FROM aggregate_config 
			WHERE cust_id = ? 
			  AND aggregate_type = ? 
			  AND name = ?`, instance.CustID, aggregateType, name,
	)
	if err != nil && err == sql.ErrNoRows {
		return aggregate.AggregateSer{}, ErrNotFound
	} else if err != nil {
		return aggregate.AggregateSer{}, err
	}
	return ret, nil
}
