package checkpoint

import (
	"database/sql"
	"fennel/instance"
	"fennel/lib/ftypes"
)

func GetCheckpoint2(this instance.Instance, aggtype ftypes.AggType, aggname ftypes.AggName) (ftypes.OidType, error) {
	row := this.DB.QueryRow(`
		SELECT checkpoint
		FROM checkpoint2
		WHERE cust_id = ?
		  AND aggtype = ?
		  AND aggname = ?;
	`, this.CustID, aggtype, aggname)
	var checkpoint uint64
	err := row.Scan(&checkpoint)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	} else if err == sql.ErrNoRows {
		// this happens when no matching row was found. By default, checkpoint is zero
		return 0, nil
	} else {
		return ftypes.OidType(checkpoint), nil
	}
}

func SetCheckpoint2(this instance.Instance, aggtype ftypes.AggType, aggname ftypes.AggName, checkpoint ftypes.OidType) error {
	_, err := this.DB.Exec(`
		INSERT INTO checkpoint2 (cust_id, aggtype, aggname, checkpoint)
        VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY
		UPDATE
			checkpoint = ?
		;`, this.CustID, aggtype, aggname, checkpoint, checkpoint)
	return err
}
