package test

import (
	"fennel/db"
	"fennel/lib/ftypes"
)

func defaultDB(tierID ftypes.TierID) (db.Connection, error) {
	// NOTE: for each test, we are creating a new tier (i.e a new set of tables
	// inside the same database. And we are never destroying these tables, which is fine
	// remember to cleanup this database every few months
	config := db.MySQLConfig{
		TierID:   tierID,
		DBname:   "fennel_test",
		Username: "admin",
		Password: "foundationdb",
		Host:     "database-nikhil-test.cluster-c00d7gkxaysk.us-west-2.rds.amazonaws.com",
		//DBname:   "fennel-test",
		//Username: "ftm4ey929riz",
		//Password: "pscale_pw_YdsInnGezBNibWLaSXzjWUNHP2ljuXGJUAq8y7iRXqQ",
		//Host:     "9kzpy3s6wi0u.us-west-2.psdb.cloud",
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
