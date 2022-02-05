package db

import (
	"database/sql"
	"fennel/lib/ftypes"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func drop(tierID ftypes.TierID, logicalname, username, password, host string) error {
	dbname := Name(tierID, logicalname)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/", username, password, host)
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
	if err := Create(tierID, logicalname, username, password, host); err != nil {
		return nil, err
	}
	dbname := Name(tierID, logicalname)
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, dbname)
	return sqlx.Open("mysql", connstr)
}

func TestSyncSchema(t *testing.T) {
	// get default DB
	rand.Seed(time.Now().UnixNano())
	config := MySQLConfig{
		TierID:   ftypes.TierID(rand.Uint32()),
		DBname:   "fennel_test",
		Username: "admin",
		Password: "foundationdb",
		Host:     "database-nikhil-test.cluster-c00d7gkxaysk.us-west-2.rds.amazonaws.com",
	}
	resource, err := config.Materialize()
	assert.NoError(t, err)
	defer drop(config.TierID, config.DBname, config.Username, config.Password, config.Host)
	db := resource.(Connection)

	// version goes to zero after dropping the DB
	conn, err := recreate(config.TierID, config.DBname, config.Username, config.Password, config.Host)
	assert.NoError(t, err)
	db.DB = conn

	// since we just created a new DB, it's version starts at zero
	version, err := schemaVersion(db)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), version)

	// now we slowly apply all the schemas and it should work without any errors
	err = SyncSchema(db)
	assert.NoError(t, err)

	// version should at least be 2 because our schema has at least two statements
	version, err = schemaVersion(db)
	assert.NoError(t, err)
	assert.True(t, version >= 2)

	// and we should be able to do queries against schema_test table (which is our second table for testing)
	name, err := TieredTableName(db.TierID(), "schema_test")
	assert.NoError(t, err)
	_, err = db.Query(fmt.Sprintf("insert into %s values (?, ?);", name), 1, 2)
	assert.NoError(t, err)
	_, err = db.Query(fmt.Sprintf("insert into %s values (?, ?);", name), 3, 4)
	assert.NoError(t, err)
	row := db.QueryRow(fmt.Sprintf("select zkey + value as total from %s where zkey = 3;", name))
	var total sql.NullInt32

	row.Scan(&total)
	assert.True(t, total.Valid)
	assert.Equal(t, int32(7), total.Int32)
}
