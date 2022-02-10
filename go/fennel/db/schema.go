package db

import (
	"database/sql"
)

type Schema map[uint32]string

func initSchemaVersion(db Connection) error {
	_, err := db.Exec(`
			create table if not exists schema_version (
			version INT NOT NULL
	)`)
	return err
}

func schemaVersion(db Connection) (uint32, error) {
	row := db.QueryRow("SELECT version FROM schema_version")
	var total sql.NullInt32
	row.Scan(&total)
	if total.Valid {
		return uint32(total.Int32), nil
	} else {
		// this likely means that there was no such row and hence the table has not been initialized
		return 0, nil
	}
}

func incrementSchemaVersion(db Connection, curr uint32) error {
	var err error
	if curr == 0 {
		_, err = db.Query("INSERT INTO schema_version VALUES (?);", 1)
	} else {
		_, err = db.Query("UPDATE schema_version SET version = version + 1")
	}
	return err
}

func syncSchema(db Connection, defs Schema) error {
	if err := initSchemaVersion(db); err != nil {
		return err
	}
	curr, err := schemaVersion(db)
	if err != nil {
		return err
	}
	len_ := uint32(len(defs))
	for i := curr + 1; i <= len_; i++ {
		if _, err = db.Exec(defs[i]); err != nil {
			return err
		}
		if err = incrementSchemaVersion(db, i-1); err != nil {
			return err
		}
	}
	return nil
}
