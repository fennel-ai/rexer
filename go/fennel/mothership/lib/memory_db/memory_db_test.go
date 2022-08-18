package memory_db

import (
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestMemoryDB(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	mdb := MemoryDB{
		ClusterID:            "id",
		ClusterSecurityGroup: "group",
		Hostname:             "hostname",
	}
	assert.Positive(t, db.Create(&mdb).RowsAffected)
	assert.Positive(t, mdb.ID)
	assert.Positive(t, db.Take(&mdb, mdb.ID).RowsAffected)
	assert.Equal(t, "id", mdb.ClusterID)
	assert.Equal(t, "group", mdb.ClusterSecurityGroup)
	assert.Equal(t, "hostname", mdb.Hostname)
	assert.Positive(t, mdb.CreatedAt)
	assert.Positive(t, mdb.UpdatedAt)
	assert.Zero(t, mdb.DeletedAt)

	id := mdb.ID
	assert.Positive(t, db.Delete(&mdb).RowsAffected)
	assert.Zero(t, db.Take(&mdb, id).RowsAffected)
	assert.Positive(t, db.Unscoped().Take(&mdb, id).RowsAffected)
}
