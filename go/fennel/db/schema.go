package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type Schema map[uint32]string

func initSchemaVersion(db *sqlx.DB) error {
	_, err := db.Exec(`
			create table if not exists schema_version (
			version INT NOT NULL
	)`)
	return err
}

// schemaVersionTx returns version of the last schema updated on the database using given transaction `tx`
//
// returns error if the `schema_version` table does not exist
func schemaVersionTx(tx *sql.Tx) (uint32, error) {
	var total sql.NullInt32
	if err := tx.QueryRow("SELECT version FROM schema_version").Scan(&total); err != nil {
		// if there were no rows, do not fail
		if err != sql.ErrNoRows {
			return 0, err
		}
	}
	if total.Valid {
		return uint32(total.Int32), nil
	} else {
		// this likely means that there was no such row and hence the table has not been initialized
		return 0, nil
	}
}

// schemaVersion returns version of the last schema updated on the database
//
// returns error if the `schema_version` table does not exist
func schemaVersion(db *sqlx.DB) (uint32, error) {
	var total sql.NullInt32
	if err := db.QueryRow("SELECT version FROM schema_version").Scan(&total); err != nil {
		// if there were no rows, do not fail
		if err != sql.ErrNoRows {
			return 0, err
		}
	}
	if total.Valid {
		return uint32(total.Int32), nil
	} else {
		// this likely means that there was no such row and hence the table has not been initialized
		return 0, nil
	}
}

func incrementVersion(tx *sql.Tx, v uint32) error {
	if v == 1 {
		if _, err := tx.Exec("INSERT INTO schema_version VALUES (?);", 1); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec("UPDATE schema_version SET version = ?", v); err != nil {
			return err
		}
	}
	return nil
}

func execSchema(db *sqlx.DB, defv uint32, def string) error {
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false})
	if err != nil {
		return err
	}
	// if tx commits successfully, rollback is a no-op. In any other failure scenario, rollback the changes
	// in the txn
	defer tx.Rollback()

	v, err := schemaVersionTx(tx)
	if err != nil {
		return err
	}
	// if the schema version right now is synced to the version we are processing, execute the schema at our current version
	// and increment the version
	if v < defv {
		// this is a sanity check to ensure that we have not accidentally skipped over any schema version
		//
		// since we are executing the schemas and updating the version atomically, this should never happen
		if defv-v > 1 {
			return fmt.Errorf("found latest schema version: %+v while trying to execute schema of version: %+v. Was one of the schemas skipped?", v, defv)
		}
		if _, err := tx.Exec(def); err != nil {
			return err
		}
		if err = incrementVersion(tx, defv); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func syncSchema(db *sqlx.DB, defs Schema) error {
	// initializing `schema_version` table is an idempotent operation
	if err := initSchemaVersion(db); err != nil {
		return err
	}

	// NOTE: since we are fetching the schema version outside of a txn, the value could change b/w this call
	// and subsequent call below
	//
	// we could essentially start the schema version from '0' explicitly, but we fetch the schema version at the current
	// time to avoid DB roundtrips (helps improve tier initialization time)
	curr, err := schemaVersion(db)
	if err != nil {
		return err
	}
	len_ := uint32(len(defs))
	for i := curr + 1; i <= len_; i++ {
		if err := execSchema(db, i, defs[i]); err != nil {
			return err
		}
	}
	return nil
}
