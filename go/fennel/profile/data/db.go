package data

import (
	"fennel/profile/lib"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	TableName string
	DB        *sqlx.DB
}

func (db DB) Name() string {
	return db.TableName
}

var _ Provider = DB{"", nil}

func (db DB) Init() error {
	sql := fmt.Sprintf(`CREATE TABLE %s (
		otype integer not null,
		oid integer not null,
		zkey varchar not null,
		version integer not null,
		value blob not null
	  );`, db.TableName)

	statement, err := db.DB.Prepare(sql)
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	//log.Println("'profile' table created")
	return nil
}

func (db DB) Set(otype lib.OType, oid lib.OidType, key string, version uint64, valueSer []byte) error {
	if version == 0 {
		return fmt.Errorf("version can not be zero")
	}
	_, err := db.DB.Exec(fmt.Sprintf(`
		INSERT INTO %s
			(otype, oid, zkey, version, value) 
		VALUES
			(?, ?, ?, ?, ?);`, db.TableName),
		otype, oid, key, version, valueSer)
	if err != nil {
		return err
	}
	return nil
}

func (db DB) Get(otype lib.OType, oid lib.OidType, key string, version uint64) ([]byte, error) {
	var value [][]byte

	var err error
	if version > 0 {
		err = db.DB.Select(&value, fmt.Sprintf(`
		SELECT value
		FROM %s
		WHERE
			otype = ? 
			AND oid = ?
			AND zkey = ?
			AND version = ?
		LIMIT 1
		`, db.TableName),
			otype, oid, key, version)
	} else {
		// if version isn't given, just pick the highest version
		err = db.DB.Select(&value, fmt.Sprintf(`
		SELECT value
		FROM %s
		WHERE
			otype = ?
			AND oid = ?
			AND zkey = ?
		ORDER BY version DESC
		LIMIT 1
		`, db.TableName),
			otype, oid, key)
	}
	if err != nil {
		return nil, err
	} else if len(value) == 0 {
		// i.e no valid value is found, so we return nil
		return nil, nil
	} else {
		return value[0], nil
	}
}
