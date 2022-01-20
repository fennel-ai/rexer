package db

import (
	"fennel/resource"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
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
	conn := Connection{config: conf, DB: DB}
	if err = SyncSchema(conn); err != nil {
		return nil, err
	}
	return conn, nil
}

var _ resource.Config = SQLiteConfig{""}

//=================================
// MySQL config for db connection
//=================================

type MySQLConfig struct {
	DBname   string
	Username string
	Password string
	Host     string
}

var _ resource.Config = MySQLConfig{}

func (conf MySQLConfig) Materialize() (resource.Resource, error) {
	connectStr := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?tls=true",
		conf.Username, conf.Password, conf.Host, conf.DBname,
	)

	DB, err := sqlx.Open("mysql", connectStr)
	if err != nil {
		return nil, err
	}
	conn := Connection{config: conf, DB: DB}
	if err = SyncSchema(conn); err != nil {
		return nil, err
	}
	return conn, nil
}

// TODO: replace this with config of a locally running MySQL process
var TestMySQLConfig = MySQLConfig{
	DBname:   "fennel-test",
	Username: "ftm4ey929riz",
	Password: "pscale_pw_YdsInnGezBNibWLaSXzjWUNHP2ljuXGJUAq8y7iRXqQ",
	Host:     "9kzpy3s6wi0u.us-west-2.psdb.cloud",
}
