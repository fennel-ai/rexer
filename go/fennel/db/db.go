package db

import (
	"fennel/lib/ftypes"
	"fennel/resource"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type Connection struct {
	config  resource.Config
	planeID ftypes.PlaneID
	*sqlx.DB
}

func (c Connection) PlaneID() ftypes.PlaneID {
	return c.planeID
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
	dbname  string
	planeID ftypes.PlaneID
}

func (conf SQLiteConfig) Materialize() (resource.Resource, error) {
	if conf.planeID == 0 {
		return nil, fmt.Errorf("plane ID not initialized")
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
	conn := Connection{config: conf, DB: DB, planeID: conf.planeID}
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
	PlaneID  ftypes.PlaneID
	DBname   string
	Username string
	Password string
	Host     string
}

var _ resource.Config = MySQLConfig{}

func (conf MySQLConfig) Materialize() (resource.Resource, error) {
	if conf.PlaneID == 0 {
		return Connection{}, fmt.Errorf("plane ID not specified")
	}
	connectStr := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?tls=true",
		conf.Username, conf.Password, conf.Host, conf.DBname,
	)
	log.Printf("connecting to db at %s", connectStr)

	DB, err := sqlx.Open("mysql", connectStr)
	if err != nil {
		return nil, err
	}
	conn := Connection{config: conf, DB: DB, planeID: conf.PlaneID}
	if err = SyncSchema(conn); err != nil {
		return nil, err
	}
	return conn, nil
}
