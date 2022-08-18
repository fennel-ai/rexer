package elasticache

import (
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestElastiCache(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	ec := ElastiCache{
		ClusterID:            "id",
		ClusterSecurityGroup: "group",
		PrimaryHostname:      "phostname",
		ReplicaHostname:      "rhostname",
	}
	assert.Positive(t, db.Create(&ec).RowsAffected)
	assert.Positive(t, ec.ID)
	assert.Positive(t, db.Take(&ec, ec.ID).RowsAffected)
	assert.Equal(t, "id", ec.ClusterID)
	assert.Equal(t, "group", ec.ClusterSecurityGroup)
	assert.Equal(t, "phostname", ec.PrimaryHostname)
	assert.Equal(t, "rhostname", ec.ReplicaHostname)
	assert.Positive(t, ec.CreatedAt)
	assert.Positive(t, ec.UpdatedAt)
	assert.Zero(t, ec.DeletedAt)

	id := ec.ID
	assert.Positive(t, db.Delete(&ec).RowsAffected)
	assert.Zero(t, db.Take(&ec, id).RowsAffected)
	assert.Positive(t, db.Unscoped().Take(&ec, id).RowsAffected)
}
