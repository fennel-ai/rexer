package test

import (
	"fennel/db"
	"fennel/instance"
)

func DefaultInstance() (instance.Instance, error) {
	db, err := DefaultDB()
	if err != nil {
		return instance.Instance{}, err
	}
	return instance.Instance{DB: db}, nil
}

func DefaultDB() (db.Connection, error) {
	resource, err := testMySQLConfig.Materialize()
	if err != nil {
		return db.Connection{}, err
	}
	db := resource.(db.Connection)
	return db, nil
}

var testMySQLConfig = db.MySQLConfig{
	DBname:   "fennel-test",
	Username: "ftm4ey929riz",
	Password: "pscale_pw_YdsInnGezBNibWLaSXzjWUNHP2ljuXGJUAq8y7iRXqQ",
	Host:     "9kzpy3s6wi0u.us-west-2.psdb.cloud",
}

//func DefaultDB() (db.Connection, error) {
//	panic("not impelemented")
//
//}
