package db

import (
	"database/sql"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func create(tierID ftypes.TierID, logicalname, username, password, host string) error {
	dbname := resource.TieredName(tierID, logicalname)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/?tls=true", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + dbname)
	return err
}

func drop(tierID ftypes.TierID, logicalname, username, password, host string) error {
	dbname := resource.TieredName(tierID, logicalname)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/?tls=true", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("DROP DATABASE IF EXISTS " + dbname)
	return err
}

func recreate(tierID ftypes.TierID, logicalname, username, password, host string) (*sqlx.DB, error) {
	if err := drop(tierID, logicalname, username, password, host); err != nil {
		return nil, err
	}
	if err := create(tierID, logicalname, username, password, host); err != nil {
		return nil, err
	}
	dbname := resource.TieredName(tierID, logicalname)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=true", username, password, host, dbname)
	return sqlx.Open("mysql", connstr)
}

func TestSyncSchema(t *testing.T) {
	// get default DB
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.TierID(rand.Uint32())
	config := MySQLConfig{
		DBname:   "schema_test",
		Username: "admin",
		Password: "foundationdb",
		Host:     "database-nikhil-test.cluster-c00d7gkxaysk.us-west-2.rds.amazonaws.com",
		Schema: Schema{
			1: `CREATE TABLE IF NOT EXISTS schema_test (
			zkey INT NOT NULL,
			value INT NOT NULL
	   );`},
	}
	// create the DB before materializing a connection
	err := create(tierID, config.DBname, config.Username, config.Password, config.Host)
	assert.NoError(t, err)

	resource, err := config.Materialize(tierID)
	assert.NoError(t, err)
	defer drop(tierID, config.DBname, config.Username, config.Password, config.Host)
	db := resource.(Connection)

	// version goes to zero after dropping the DB
	conn, err := recreate(tierID, config.DBname, config.Username, config.Password, config.Host)
	assert.NoError(t, err)
	db.DB = conn

	// since we just created a new DB, it's version starts at zero
	version, err := schemaVersion(db)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), version)

	// now we slowly apply all the schemas and it should work without any errors
	err = syncSchema(db, config.Schema)
	assert.NoError(t, err)

	// version should be at one now
	version, err = schemaVersion(db)
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
