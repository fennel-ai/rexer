package db

import (
	"database/sql"
	"fennel/lib/ftypes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

// dropDB drops all tables in the given database.
func dropDB(db Connection) error {
	for _, name := range tablenames {
		ptablename, err := ToPlaneTablename(db.PlaneID(), name)
		if err != nil {
			return err
		}
		_, err = db.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s;", ptablename))
		if err != nil {
			return err
		}
	}
	return nil
}

func TestSyncSchema(t *testing.T) {
	// get default DB
	rand.Seed(time.Now().UnixNano())
	config := MySQLConfig{
		PlaneID:  ftypes.PlaneID(rand.Uint32()),
		DBname:   "fennel-test",
		Username: "ftm4ey929riz",
		Password: "pscale_pw_YdsInnGezBNibWLaSXzjWUNHP2ljuXGJUAq8y7iRXqQ",
		Host:     "9kzpy3s6wi0u.us-west-2.psdb.cloud",
	}
	resource, err := config.Materialize()
	assert.NoError(t, err)
	db := resource.(Connection)

	// version goes to zero after dropping the DB
	assert.NoError(t, dropDB(db))

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
