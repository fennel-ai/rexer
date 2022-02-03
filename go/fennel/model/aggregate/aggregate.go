package aggregate

import (
	"database/sql"
	"fennel/db"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/plane"
	"fmt"
)

func Store(plane plane.Plane, aggtype ftypes.AggType, name ftypes.AggName, querySer []byte, ts ftypes.Timestamp, optionSer []byte) error {
	if len(aggtype) > 255 {
		return fmt.Errorf("aggregate type can not be longer than 255 chars")
	}
	if len(name) > 255 {
		return fmt.Errorf("aggregate name can not be longer than 255 chars")
	}
	tablename, err := planeTable(plane.ID)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?)`, tablename)
	_, err = plane.DB.Query(sql, plane.CustID, aggtype, name, querySer, ts, optionSer)
	return err
}

func Retrieve(plane plane.Plane, aggregateType ftypes.AggType, name ftypes.AggName) (aggregate.AggregateSer, error) {
	var ret aggregate.AggregateSer
	tablename, err := planeTable(plane.ID)
	if err != nil {
		return ret, err
	}
	err = plane.DB.Get(&ret, fmt.Sprintf(`
			SELECT * FROM %s 
			WHERE cust_id = ? 
			  AND aggregate_type = ? 
			  AND name = ?`, tablename),
		plane.CustID, aggregateType, name,
	)
	if err != nil && err == sql.ErrNoRows {
		return aggregate.AggregateSer{}, aggregate.ErrNotFound
	} else if err != nil {
		return aggregate.AggregateSer{}, err
	}
	return ret, nil
}

func RetrieveAll(plane plane.Plane, aggtype ftypes.AggType) ([]aggregate.AggregateSer, error) {
	ret := make([]aggregate.AggregateSer, 0)
	tablename, err := planeTable(plane.ID)
	if err != nil {
		return ret, err
	}
	err = plane.DB.Select(&ret, fmt.Sprintf(`
			SELECT * FROM %s 
			WHERE cust_id = ? 
			  AND aggregate_type = ? 
			  `, tablename),
		plane.CustID, aggtype,
	)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func planeTable(planeID ftypes.PlaneID) (string, error) {
	return db.ToPlaneTablename(planeID, "aggregate_config")
}
