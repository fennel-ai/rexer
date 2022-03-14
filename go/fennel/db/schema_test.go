package db

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/resource"
)

const (
	host = "fenneldb-20220314043639794500000002.cluster-c00d7gkxaysk.us-west-2.rds.amazonaws.com"
)

func create(dbname, username, password, host string) error {
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/?tls=true", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + dbname)
	return err
}

func drop(dbname, username, password, host string) error {
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/?tls=true", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("DROP DATABASE IF EXISTS " + dbname)
	return err
}

func recreate(dbname, username, password, host string) (*sqlx.DB, error) {
	if err := drop(dbname, username, password, host); err != nil {
		return nil, err
	}
	if err := create(dbname, username, password, host); err != nil {
		return nil, err
	}
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=true", username, password, host, dbname)
	return sqlx.Open("mysql", connstr)
}

func TestSyncSchema(t *testing.T) {
	// get default DB
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	config := MySQLConfig{
		DBname:   scope.PrefixedName("schema_test"),
		Username: "admin",
		Password: "foundationdb",
		Host:     host,
		Schema: Schema{
			1: `CREATE TABLE IF NOT EXISTS schema_test (
					zkey INT NOT NULL,
					value INT NOT NULL
			);`,
		},
		Scope: scope,
	}
	// create the DB before materializing a connection
	err := create(config.DBname, config.Username, config.Password, config.Host)
	assert.NoError(t, err)

	resource, err := config.Materialize()
	assert.NoError(t, err)
	defer drop(config.DBname, config.Username, config.Password, config.Host)
	db := resource.(Connection)

	// version goes to zero after dropping the DB
	conn, err := recreate(config.DBname, config.Username, config.Password, config.Host)
	assert.NoError(t, err)
	db.DB = conn

	// since we just created a new DB, it's version starts at zero
	version, err := schemaVersion(db.DB)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), version)

	// now we slowly apply all the schemas and it should work without any errors
	err = syncSchema(db.DB, config.Schema)
	assert.NoError(t, err)

	// version should be at one now
	version, err = schemaVersion(db.DB)
	assert.NoError(t, err)
	assert.True(t, version == 1)

	// and we should be able to do queries against schema_test table
	_, err = db.Query("insert into schema_test values (?, ?);", 1, 2)
	assert.NoError(t, err)
	_, err = db.Query("insert into schema_test values (?, ?);", 3, 4)
	assert.NoError(t, err)
	row := db.QueryRow("select zkey + value as total from schema_test where zkey = 3;")
	var total sql.NullInt32

	row.Scan(&total)
	assert.True(t, total.Valid)
	assert.Equal(t, int32(7), total.Int32)
}
