package main

import (
	. "fennel/profile/lib"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
)

var DB *sqlx.DB = nil

const (
	DBNAME    = "fennel.db"
	TABLENAME = "profile"
)

func dbInit() {
	os.Remove(DBNAME)

	log.Printf("Creating db file %s...\n", DBNAME)
	file, err := os.Create(DBNAME)
	if err != nil {
		panic(err)
	}
	file.Close()
	log.Printf("%s created\n", DBNAME)

	DB, err = sqlx.Open("sqlite3", fmt.Sprintf("./%s", DBNAME))
	if err != nil {
		panic(err)
	}
	// now actually create the table
	sql := fmt.Sprintf(`CREATE TABLE %s (
		"otype" integer not null,
		"oid" integer not null,
		"key" varchar not null,
		"version" integer not null,
		"value" blob not null
	  );`, TABLENAME)

	log.Println("Creating profile table...", sql)
	statement, err := DB.Prepare(sql)
	if err != nil {
		panic(err)
	}
	statement.Exec()
	log.Println("Profile table created")
}

func dbShutdown() {
	DB.Close()
}

func dbSet(otype OType, oid uint64, key string, version uint64, value []byte) error {
	if version == 0 {
		return fmt.Errorf("version can not be zero")
	}
	//log.Printf("Inserting %v in table %s...\n", item, TABLENAME)
	_, err := DB.Exec(fmt.Sprintf(`
		INSERT INTO %s 
			(otype, oid, key, version, value) 
		VALUES
			(?, ?, ?, ?, ?);`, TABLENAME),
		//(:otype, :oid, :key, :version, :value);`, TABLENAME),
		otype, oid, key, version, value)
	if err != nil {
		return err
	}
	return nil
}

func dbGet(otype OType, oid uint64, key string, version uint64) ([]byte, error) {
	// returns empty string if the row wasn't found
	var value [][]byte
	var err error
	if version > 0 {
		err = DB.Select(&value, fmt.Sprintf(`
		SELECT value
		FROM %s
		WHERE
			otype = ? 
			AND oid = ?
			AND key = ?
			AND version = ?
		LIMIT 1
		`, TABLENAME),
			otype, oid, key, version)
	} else {
		// if version isn't given, just pick the highest version
		err = DB.Select(&value, fmt.Sprintf(`
		SELECT value
		FROM %s
		WHERE
			otype = ?
			AND oid = ?
			AND key = ?
		ORDER BY version DESC
		LIMIT 1
		`, TABLENAME),
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

func dbPrintAll() error {
	// this is slow and will do full table scan. Just use it for debugging/dev
	var items []ProfileItemSer
	err := DB.Select(&items, fmt.Sprintf("SELECT * FROM %s", TABLENAME))
	if err != nil {
		return err
	}
	for _, item := range items {
		fmt.Printf("%#v\n", item)
	}
	return nil
}
