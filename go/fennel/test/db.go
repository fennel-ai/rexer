package test

import (
	"fmt"

	"fennel/db"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fennel/tier"

	"github.com/jmoiron/sqlx"
)

func defaultDB(tierID ftypes.RealmID, logicalname, username, password, host string) (db.Connection, error) {
	scope := resource.NewTierScope(tierID)
	if err := setupDB(tierID, logicalname, username, password, host); err != nil {
		return db.Connection{}, err
	}
	config := db.MySQLConfig{
		DBname:   scope.PrefixedName(logicalname),
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

func drop(tierID ftypes.RealmID, logicalname, username, password, host string) error {
	scope := resource.NewTierScope(tierID)
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

func setupDB(tierID ftypes.RealmID, logicalname, username, password, host string) error {
	scope := resource.NewTierScope(tierID)
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
