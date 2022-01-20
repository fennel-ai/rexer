package test

import (
	"fennel/db"
)

func DefaultDB() (db.Connection, error) {
	// TODO: start creating a separate dB (with a random name) for each test
	// that will allow us to run go tests in parallel (which is currently disabled
	// in testall.py)
	resource, err := db.TestMySQLConfig.Materialize()
	if err != nil {
		return db.Connection{}, err
	}
	DB := resource.(db.Connection)

	// we recreate the DB for each test
	DB, err = db.Recreate_I_KNOW_WHAT_IM_DOING(DB)
	if err != nil {
		return db.Connection{}, err
	}
	if err = db.SyncSchema(DB); err != nil {
		return db.Connection{}, err
	}
	return DB, nil
}
