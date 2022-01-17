package db

import (
	"fennel/instance"
	"fennel/resource"
	"fmt"
	"github.com/jmoiron/sqlx"
	"os"
)

type Connection struct {
	config resource.Config
	*sqlx.DB
}

func (c Connection) Teardown() error {
	if config, ok := c.config.(SQLiteConfig); ok {
		dbname := config.dbname
		os.Remove(dbname)
	}
	return nil
}

func (c Connection) Close() error {
	return c.Close()
}

func (c Connection) Type() resource.Type {
	return resource.DBConnection
}

func Default() (resource.Resource, error) {
	switch instance.Current() {
	case instance.TEST:
		return SQLiteConfig{"fennel.db"}.Materialize()
	case instance.PROD:
		return testMySQLConfig.Materialize()
	default:
		return nil, fmt.Errorf("invalid instance")
	}
}

//=================================
// SQLite config for db connection
//=================================

type SQLiteConfig struct {
	dbname string
}

func (conf SQLiteConfig) Materialize() (resource.Resource, error) {
	dbname := conf.dbname
	os.Remove(dbname)

	file, err := os.Create(dbname)
	if err != nil {
		return nil, err
	}
	file.Close()

	DB, err := sqlx.Open("sqlite3", fmt.Sprintf("./%s", dbname))
	if err != nil {
		return nil, err
	}
	return Connection{conf, DB}, nil
}

var _ resource.Config = SQLiteConfig{""}

//=================================
// MySQL config for db connection
//=================================

type MySQLConfig struct {
	dbname   string
	username string
	password string
	host     string
}

var _ resource.Config = MySQLConfig{}

var testMySQLConfig = MySQLConfig{
	dbname:   "fennel-test",
	username: "ftm4ey929riz",
	password: "pscale_pw_YdsInnGezBNibWLaSXzjWUNHP2ljuXGJUAq8y7iRXqQ",
	host:     "9kzpy3s6wi0u.us-west-2.psdb.cloud",
}

func (conf MySQLConfig) Materialize() (resource.Resource, error) {
	connectStr := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?tls=true",
		conf.username, conf.password, conf.host, conf.dbname,
	)

	DB, err := sqlx.Open("mysql", connectStr)
	if err != nil {
		return nil, err
	}
	return Connection{conf, DB}, nil
}
