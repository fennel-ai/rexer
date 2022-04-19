package db

import (
	"database/sql"
	"fmt"

	"fennel/lib/utils"

	"github.com/jmoiron/sqlx"
)

type Schema map[uint32]string

// initSchema initializes the schema for the database.
// it does so by creating the schema table if it does not exist and populating it with the
// default version of 0. This works even when multiple processes are trying to create the
// schema table at the same time
func initSchemaVersion(db *sqlx.DB) error {
	_, err := db.Exec(`
			create table if not exists schema_version (
				 version INT NOT NULL
			)`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`
		  INSERT INTO schema_version (version) 
		  SELECT 0 WHERE NOT EXISTS (SELECT * FROM schema_version);
	  `)
	return err
}

// schemaVersion returns version of the last schema updated on the database
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

func syncSchema(db *sqlx.DB, defs Schema) error {
	// initializing `schema_version` table is an idempotent operation
	if err := initSchemaVersion(db); err != nil {
		return err
	}
	start, err := schemaVersion(db)
	if err != nil {
		return err
	}
	len_ := uint32(len(defs))
	// nothing to update
	if start >= len_ {
		return nil
	}

	name := fmt.Sprintf("update_schema_%s", utils.RandString(8))
	defer db.Exec(fmt.Sprintf("DROP PROCEDURE IF EXISTS %s", name))

	// starting from version next to 'start' so that we don't have to recreate the existing schema
	for version := start + 1; version <= len_; version++ {
		sql := defs[version]
		if _, err := db.Exec(fmt.Sprintf("DROP PROCEDURE IF EXISTS %s", name)); err != nil {
			return err
		}
		procedure := fmt.Sprintf(`
		 CREATE PROCEDURE %s ()
		 BEGIN
			IF (SELECT version FROM schema_version) = %d - 1 
			THEN
				 UPDATE schema_version SET version = version + 1;
				 %s
			END IF;
		 END;`, name, version, sql)
		if _, err = db.Exec(procedure); err != nil {
			return fmt.Errorf("could not define procedure: %v", err)
		}
		if _, err = db.Exec(fmt.Sprintf("CALL %s()", name)); err != nil {
			return fmt.Errorf("could not execute procedure: %v", err)
		}
	}
	return nil
}
