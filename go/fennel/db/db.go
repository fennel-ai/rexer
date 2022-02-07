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

func (c Connection) Close() error {
	return c.Close()
}

func (c Connection) Type() resource.Type {
	return resource.DBConnection
}

var _ resource.Resource = Connection{}

//=================================
// SQLite Config for db connection
//=================================

type SQLiteConfig struct {
	dbname string
}

func (conf SQLiteConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	dbname := resource.TieredName(tierID, conf.dbname)

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
	conn := Connection{config: conf, DB: DB, tierID: tierID}
	if err = SyncSchema(conn); err != nil {
		return nil, err
	}
	return conn, nil
}

var _ resource.Config = SQLiteConfig{}

//=================================
// MySQL Config for db connection
//=================================

type MySQLConfig struct {
	DBname   string
	Username string
	Password string
	Host     string
}

var _ resource.Config = MySQLConfig{}

func (conf MySQLConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return Connection{}, fmt.Errorf("tier ID not specified")
	}
	dbname := resource.TieredName(tierID, conf.DBname)
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=true", conf.Username, conf.Password, conf.Host, dbname)
	DB, err := sqlx.Open("mysql", connectStr)
	if err != nil {
		return nil, err
	}

	conn := Connection{config: conf, DB: DB, tierID: tierID}
	if err := SyncSchema(conn); err != nil {
		return nil, err
	}
	return conn, nil
}
