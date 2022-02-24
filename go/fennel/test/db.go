package test

import (
	"fmt"

	"fennel/db"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fennel/tier"

	"github.com/jmoiron/sqlx"
)

const (
	username            = "admin"
	password            = "foundationdb"
	host                = "database-nikhil-test.cluster-c00d7gkxaysk.us-west-2.rds.amazonaws.com"
	logical_test_dbname = "testdb"
)

func defaultDB(tierID ftypes.TierID) (db.Connection, error) {
	scope := resource.NewTierScope(1, tierID)
	if err := setupDB(tierID, logical_test_dbname, username, password, host); err != nil {
		return db.Connection{}, err
	}
	config := db.MySQLConfig{
		DBname:   scope.PrefixedName(logical_test_dbname),
		Username: username,
		Password: password,
		Host:     host,
		Schema:   tier.Schema,
		Scope:    scope,
	}
	resource, err := config.Materialize()
	if err != nil {
		return db.Connection{}, err
	}
	DB := resource.(db.Connection)

	return DB, nil
}

func drop(tierID ftypes.TierID, logicalname, username, password, host string) error {
	scope := resource.NewTierScope(1, tierID)
	dbname := scope.PrefixedName(logicalname)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/?tls=true", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("DROP DATABASE IF EXISTS " + dbname)
	return err
}

func setupDB(tierID ftypes.TierID, logicalname, username, password, host string) error {
	scope := resource.NewTierScope(1, tierID)
	dbname := scope.PrefixedName(logicalname)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/?tls=true", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return fmt.Errorf("could not open DB: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + dbname)
	return err
}
