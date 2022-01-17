package data

import (
	"fennel/db"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

type ProfileTable struct {
	db.Table
}

func NewProfileTable(conn db.Connection) (ProfileTable, error) {
	name := "profile"
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		otype integer not null,
		oid integer not null,
		zkey varchar(256) not null,
		version integer not null,
		value blob not null
	  );`, name)
	conf := db.TableConfig{SQL: sql, Name: name, DB: conn, DropTable: true}
	table, err := conf.Materialize()
	if err != nil {
		return ProfileTable{}, err
	}
	return ProfileTable{table.(db.Table)}, nil
}

func (table ProfileTable) Name() string {
	return table.Table.Name
}

var _ Provider = ProfileTable{}

func (table ProfileTable) Init() error {
	return nil
}

func (table ProfileTable) Set(otype uint32, oid uint64, key string, version uint64, valueSer []byte) error {
	if version == 0 {
		return fmt.Errorf("version can not be zero")
	}
	if len(key) > 256 {
		return fmt.Errorf("key too long: keys can only be upto 256 chars")
	}
	_, err := table.DB.Exec(fmt.Sprintf(`
		INSERT INTO %s
			(otype, oid, zkey, version, value) 
		VALUES
			(?, ?, ?, ?, ?);`, table.Name()),
		otype, oid, key, version, valueSer)
	if err != nil {
		return err
	}
	return nil
}

func (table ProfileTable) Get(otype uint32, oid uint64, key string, version uint64) ([]byte, error) {
	var value [][]byte

	var err error
	if version > 0 {
		err = table.DB.Select(&value, fmt.Sprintf(`
		SELECT value
		FROM %s
		WHERE
			otype = ? 
			AND oid = ?
			AND zkey = ?
			AND version = ?
		LIMIT 1
		`, table.Name()),
			otype, oid, key, version)
	} else {
		// if version isn't given, just pick the highest version
		err = table.DB.Select(&value, fmt.Sprintf(`
		SELECT value
		FROM %s
		WHERE
			otype = ?
			AND oid = ?
			AND zkey = ?
		ORDER BY version DESC
		LIMIT 1
		`, table.Name()),
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
