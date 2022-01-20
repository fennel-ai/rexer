package db

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSyncSchema(t *testing.T) {
	// get default DB
	resource, err := TestMySQLConfig.Materialize()
	assert.NoError(t, err)
	db := resource.(Connection)

	// if we recreate the db, its version gets reset to zero
	db, err = Recreate_I_KNOW_WHAT_IM_DOING(db)
	assert.NoError(t, err)
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
