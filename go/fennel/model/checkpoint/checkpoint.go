package checkpoint

import (
	"database/sql"
	"fennel/data/lib"
	"fennel/instance"
	lib2 "fennel/profile/lib"
)

func GetCheckpoint(this instance.Instance, ct lib.CounterType) (lib2.OidType, error) {
	row := this.DB.QueryRow(`
		SELECT checkpoint
		FROM checkpoint
		WHERE counter_type = ?;
	`, ct)
	var checkpoint uint64
	err := row.Scan(&checkpoint)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	} else if err == sql.ErrNoRows {
		// this happens when no matching row was found. By default, checkpoint is zero
		return 0, nil
	} else {
		return lib2.OidType(checkpoint), nil
	}
}

func SetCheckpoint(this instance.Instance, ct lib.CounterType, checkpoint lib2.OidType) error {
	_, err := this.DB.Exec(`
		INSERT INTO checkpoint (counter_type, checkpoint)
        VALUES (?, ?)
		ON DUPLICATE KEY
		UPDATE
			checkpoint = ?
		;`, ct, checkpoint, checkpoint)
	return err
}
