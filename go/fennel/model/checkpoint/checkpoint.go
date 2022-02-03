package checkpoint

import (
	"database/sql"
	"fennel/db"
	"fennel/lib/ftypes"
	"fennel/plane"
	"fmt"
)

func Get(this plane.Plane, aggtype ftypes.AggType, aggname ftypes.AggName) (ftypes.OidType, error) {
	tablename, err := planeTable(this.ID)
	if err != nil {
		return 0, err
	}
	row := this.DB.QueryRow(fmt.Sprintf(`
		SELECT checkpoint
		FROM %s
		WHERE cust_id = ?
		  AND aggtype = ?
		  AND aggname = ?;
	`, tablename),
		this.CustID, aggtype, aggname,
	)
	var checkpoint uint64
	err = row.Scan(&checkpoint)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	} else if err == sql.ErrNoRows {
		// this happens when no matching row was found. By default, checkpoint is zero
		return 0, nil
	} else {
		return ftypes.OidType(checkpoint), nil
	}
}

func Set(this plane.Plane, aggtype ftypes.AggType, aggname ftypes.AggName, checkpoint ftypes.OidType) error {
	tablename, err := planeTable(this.ID)
	if err != nil {
		return err
	}
	_, err = this.DB.Exec(fmt.Sprintf(`
		INSERT INTO %s (cust_id, aggtype, aggname, checkpoint)
        VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY
		UPDATE
			checkpoint = ?
		;`, tablename),
		this.CustID, aggtype, aggname, checkpoint, checkpoint,
	)
	return err
}

func planeTable(planeID ftypes.PlaneID) (string, error) {
	return db.ToPlaneTablename(planeID, "checkpoint")
}
