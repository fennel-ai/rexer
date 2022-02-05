package db

import (
	"fennel/lib/ftypes"
	"fennel/resource"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

type Connection struct {
	config resource.Config
	tierID ftypes.TierID
	*sqlx.DB
}

func (c Connection) TierID() ftypes.TierID {
	return c.tierID
}

func (c Connection) Teardown() error {
	return nil
}

func (c Connection) Close() error {
	return c.Close()
}

func (c Connection) Type() resource.Type {
	return resource.DBConnection
}

var _ resource.Resource = Connection{}

//=================================
// SQLite config for db connection
//=================================

type SQLiteConfig struct {
	dbname string
	TierID ftypes.TierID
}

func (conf SQLiteConfig) Materialize() (resource.Resource, error) {
	if conf.TierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
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
	conn := Connection{config: conf, DB: DB, tierID: conf.TierID}
	if err = SyncSchema(conn); err != nil {
		return nil, err
	}
	return conn, nil
}

var _ resource.Config = SQLiteConfig{}

//=================================
// MySQL config for db connection
//=================================

type MySQLConfig struct {
	TierID   ftypes.TierID
	DBname   string
	Username string
	Password string
	Host     string
}

var _ resource.Config = MySQLConfig{}

func (conf MySQLConfig) Materialize() (resource.Resource, error) {
	if conf.TierID == 0 {
		return Connection{}, fmt.Errorf("tier ID not specified")
	}
	connectStr := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?tls=true",
		conf.Username, conf.Password, conf.Host, conf.DBname,
	)
	DB, err := sqlx.Open("mysql", connectStr)
	if err != nil {
		return nil, err
	}

	conn := Connection{config: conf, DB: DB, tierID: conf.TierID}
	if err := SyncSchema(conn); err != nil {
		return nil, err
	}
	return conn, nil
}
