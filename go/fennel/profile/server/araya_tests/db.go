package main

import (
    "fmt"
    "log"
	"github.com/jmoiron/sqlx"
    _ "github.com/go-sql-driver/mysql"
)

const (
	TABLENAME = "profile"
)

var DB *sqlx.DB = nil

func dbClear() {
    sql := fmt.Sprintf(`DROP TABLE %s`, TABLENAME)
    
    log.Println("Deleting profile table...", sql)
    query, err := DB.Query(sql)
    if err != nil {
        panic(err)
    }
    log.Println("Profile table deleted")
    query.Close();
}

func dbInit() {
    var err error
	DB, err = sqlx.Open("mysql", "ftm4ey929riz:pscale_pw_YdsInnGezBNibWLaSXzjWUNHP2ljuXGJUAq8y7iRXqQ@tcp(9kzpy3s6wi0u.us-west-2.psdb.cloud)/fennel-test?tls=true")
    
	if err != nil {
		panic(err)
	}
    log.Println("Ready");
    
    dbClear()
    
    // now actually create the table
	sql := fmt.Sprintf(`CREATE TABLE %s (
		otype integer not null,
		oid integer not null,
		zkey varchar(255) not null,
		version integer not null,
		value blob not null
	  );`, TABLENAME)

	log.Println("Creating profile table...", sql)
	query, err := DB.Query(sql)
	if err != nil {
		panic(err)
	}
	log.Println("Profile table created")
	query.Close();
}

func dbSet(otype uint64, oid uint64, key string, version uint64, value []byte) error {
	if version == 0 {
		return fmt.Errorf("version can not be zero")
	}
	//log.Printf("Inserting %v in table %s...\n", item, TABLENAME)
	_, err := DB.Exec(fmt.Sprintf(`
		INSERT INTO %s 
			(otype, oid, zkey, version, value) 
		VALUES
			(?, ?, ?, ?, ?);`, TABLENAME),
		//(:otype, :oid, :key, :version, :value);`, TABLENAME),
		otype, oid, key, version, value)
	if err != nil {
		return err
	}
	return nil
}

func dbGet(otype uint64, oid uint64, key string, version uint64) ([]byte, error) {
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
			AND zkey = ?
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
			AND zkey = ?
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

func dbShutdown() {    
	DB.Close()
}