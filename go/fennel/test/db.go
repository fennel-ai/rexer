package test

import (
	"fennel/db"
	"fennel/lib/ftypes"
)

func defaultDB(planeID ftypes.PlaneID) (db.Connection, error) {
	// NOTE: for each test, we are creating a new plane (i.e a new set of tables
	// inside the same database. And we are never destroying these tables, which is fine
	// remember to cleanup this database every few months
	config := db.MySQLConfig{
		PlaneID:  planeID,
		DBname:   "fennel-test",
		Username: "ftm4ey929riz",
		Password: "pscale_pw_YdsInnGezBNibWLaSXzjWUNHP2ljuXGJUAq8y7iRXqQ",
		Host:     "9kzpy3s6wi0u.us-west-2.psdb.cloud",
	}
	resource, err := config.Materialize()
	if err != nil {
		return db.Connection{}, err
	}
	DB := resource.(db.Connection)

	if err = db.SyncSchema(DB); err != nil {
		return db.Connection{}, err
	}
	return DB, nil
}
