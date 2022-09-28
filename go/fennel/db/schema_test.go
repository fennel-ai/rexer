//go:build !race

package db

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/resource"
)

var (
	host     = os.Getenv("MYSQL_SERVER_ADDRESS")
	username = os.Getenv("MYSQL_USERNAME")
	password = os.Getenv("MYSQL_PASSWORD")
)

func create(dbname, username, password, host string) error {
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + dbname)
	return err
}

func drop(dbname, username, password, host string) error {
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/", username, password, host)
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
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, dbname)
	return sqlx.Open("mysql", connstr)
}

func TestSyncSchema(t *testing.T) {
	// get default DB
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	config := MySQLConfig{
		DBname:   scope.PrefixedName("schema_test"),
		Username: username,
		Password: password,
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
	defer func() {
		err = drop(config.DBname, config.Username, config.Password, config.Host)
		assert.NoError(t, err)
	}()
	db := resource.(Connection)

	// version goes to zero after dropping the DB
	conn, err := recreate(config.DBname, config.Username, config.Password, config.Host)
	assert.NoError(t, err)
	db.DB = conn

	// since we just created a new DB, `schema_version` table does not exist
	version, err := schemaVersion(db.DB)
	assert.Error(t, err)
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

	err = row.Scan(&total)
	assert.NoError(t, err)
	assert.True(t, total.Valid)
	assert.Equal(t, int32(7), total.Int32)
}

// TODO(mohit, REX-1374): This test is flaky and often blocks the CI.
//func TestConcurrentSyncSchema(t *testing.T) {
//	// get default DB
//	rand.Seed(time.Now().UnixNano())
//	tierID := ftypes.RealmID(rand.Uint32())
//	scope := resource.NewTierScope(tierID)
//	config := MySQLConfig{
//		DBname:   scope.PrefixedName("schema_test"),
//		Username: username,
//		Password: password,
//		Host:     host,
//		// Add more schema queries to have potential overlap b/w them
//		// when two goroutines try to sync the schema
//		Schema: Schema{
//			1: `CREATE TABLE IF NOT EXISTS schema_test (
//					zkey INT NOT NULL,
//					value INT NOT NULL
//			);`,
//			2: `CREATE TABLE IF NOT EXISTS schema_test2 (
//					zkey INT NOT NULL,
//					value INT NOT NULL
//			);`,
//			3: `CREATE TABLE IF NOT EXISTS schema_test3 (
//					zkey INT NOT NULL,
//					value INT NOT NULL
//			);`,
//			4: `CREATE TABLE IF NOT EXISTS schema_test4 (
//					zkey INT NOT NULL,
//					value INT NOT NULL
//			);`,
//			5: `ALTER TABLE schema_test ADD CONSTRAINT test_unique UNIQUE (zkey, value);`,
//		},
//		Scope: scope,
//	}
//	// create the DB before materializing a connection
//	err := create(config.DBname, config.Username, config.Password, config.Host)
//	assert.NoError(t, err)
//
//	resource, err := config.Materialize()
//	assert.NoError(t, err)
//	defer func() {
//		err := drop(config.DBname, config.Username, config.Password, config.Host)
//		assert.NoError(t, err)
//	}()
//	db := resource.(Connection)
//
//	// version goes to zero after dropping the DB
//	conn, err := recreate(config.DBname, config.Username, config.Password, config.Host)
//	assert.NoError(t, err)
//	db.DB = conn
//
//	// since we just created a new DB, `schema_version` table does not exist
//	version, err := schemaVersion(db.DB)
//	assert.Error(t, err)
//	assert.Equal(t, uint32(0), version)
//
//	wg := sync.WaitGroup{}
//	wg.Add(2)
//	// spin up two routines which will both try to sync the schemas on the same DB
//	for i := 0; i < 2; i++ {
//		go func() {
//			defer wg.Done()
//			err = syncSchema(db.DB, config.Schema)
//			assert.NoError(t, err)
//			// version should be at five now
//			version, err = schemaVersion(db.DB)
//			assert.NoError(t, err)
//			assert.Equal(t, uint32(5), version)
//		}()
//	}
//	wg.Wait()
//}
