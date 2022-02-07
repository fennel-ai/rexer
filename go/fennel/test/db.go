package test

import (
	"fennel/db"
	"fennel/lib/ftypes"
	"fmt"
	"github.com/jmoiron/sqlx"
)

const (
	username            = "admin"
	password            = "foundationdb"
	host                = "database-nikhil-test.cluster-c00d7gkxaysk.us-west-2.rds.amazonaws.com"
	logical_test_dbname = "testdb"
)

func defaultDB(tierID ftypes.TierID) (db.Connection, error) {
	config := db.MySQLConfig{
		DBname:   logical_test_dbname,
		Username: username,
		Password: password,
		Host:     host,
	}
	resource, err := config.Materialize(tierID)
	if err != nil {
		return db.Connection{}, err
	}
	DB := resource.(db.Connection)

	if err = db.SyncSchema(DB); err != nil {
		return db.Connection{}, err
	}
	return DB, nil
}

func drop(tierID ftypes.TierID, logicalname, username, password, host string) error {
	dbname := db.Name(tierID, logicalname)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/?tls=true", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("DROP DATABASE IF EXISTS " + dbname)
	return err
}
