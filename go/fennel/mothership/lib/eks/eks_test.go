package eks

import (
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestEks(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	eks := Eks{
		ClusterID:    "cluster",
		MinInstances: 1,
		MaxInstances: 2,
		InstanceType: "type",
	}
	assert.Positive(t, db.Create(&eks).RowsAffected)
	assert.Positive(t, eks.ID)
	assert.Positive(t, db.Take(&eks, eks.ID).RowsAffected)
	assert.Equal(t, "cluster", eks.ClusterID)
	assert.Equal(t, uint(1), eks.MinInstances)
	assert.Equal(t, uint(2), eks.MaxInstances)
	assert.Equal(t, "type", eks.InstanceType)
	assert.Positive(t, eks.CreatedAt)
	assert.Positive(t, eks.UpdatedAt)
	assert.Zero(t, eks.DeletedAt)

	id := eks.ID
	assert.Positive(t, db.Delete(&eks).RowsAffected)
	assert.Zero(t, db.Take(&eks, id).RowsAffected)
	assert.Positive(t, db.Unscoped().Take(&eks, id).RowsAffected)
}
