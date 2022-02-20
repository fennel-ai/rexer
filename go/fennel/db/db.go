package db

import (
	"fmt"
	"os"
	"time"

	"fennel/resource"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type Connection struct {
	config resource.Config
	resource.Scope
	*sqlx.DB
}

func (c Connection) Close() error {
	return c.DB.Close()
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
	schema Schema
	scope  resource.Scope
}

func (conf SQLiteConfig) Materialize() (resource.Resource, error) {
	os.Remove(conf.dbname)

	file, err := os.Create(conf.dbname)
	if err != nil {
		return nil, err
	}
	file.Close()

	DB, err := sqlx.Connect("sqlite3", fmt.Sprintf("./%s", conf.dbname))
	if err != nil {
		return nil, err
	}
	conn := Connection{config: conf, DB: DB, Scope: conf.scope}
	if err = syncSchema(conn.DB, conf.schema); err != nil {
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
	Schema   Schema
	Scope    resource.Scope
}

var _ resource.Config = MySQLConfig{}

func (conf MySQLConfig) Materialize() (resource.Resource, error) {
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=true", conf.Username, conf.Password, conf.Host, conf.DBname)
	DB, err := sqlx.Connect("mysql", connectStr)
	if err != nil {
		return nil, err
	}
	// The default is 0 (unlimited)
	DB.SetMaxOpenConns(800)
	// defaultMaxIdleConns = 2
	DB.SetMaxIdleConns(100)
	// Use connections for an hour before expiry. This is especially useful in
	// an elastic environment like Aurora where servers might be added or removed
	// depending on load. Otherwise, connections can remain in a "broken" state and
	// cause hard-to-debug errors much later.
	DB.SetConnMaxLifetime(1 * time.Hour)

	conn := Connection{config: conf, DB: DB, Scope: conf.Scope}
	if err := syncSchema(conn.DB, conf.Schema); err != nil {
		return nil, err
	}
	return conn, nil
}
