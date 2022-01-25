package checkpoint

import (
	"database/sql"
	"fennel/instance"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
)

func GetCheckpoint(this instance.Instance, ct counter.CounterType) (ftypes.OidType, error) {
	// TODO: Use appropriate custid instead
	row := this.DB.QueryRow(`
		SELECT checkpoint
		FROM checkpoint
		WHERE cust_id = ?
		AND counter_type = ?;
	`, this.CustID, ct)
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

func SetCheckpoint(this instance.Instance, ct counter.CounterType, checkpoint ftypes.OidType) error {
	// TODO: Use appropriate custid instead
	_, err := this.DB.Exec(`
		INSERT INTO checkpoint (cust_id, counter_type, checkpoint)
        VALUES (?, ?, ?)
		ON DUPLICATE KEY
		UPDATE
			checkpoint = ?
		;`, this.CustID, ct, checkpoint, checkpoint)
	return err
}
