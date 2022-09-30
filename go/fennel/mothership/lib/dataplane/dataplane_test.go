package dataplane

import (
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestDataplane(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	dp := DataPlane{
		AwsRole:     "role",
		Region:      "region",
		PulumiStack: "pulumi",
		VpcID:       "vpc",
	}

	assert.Positive(t, db.Create(&dp).RowsAffected)
	assert.Positive(t, dp.ID)
	assert.Positive(t, db.Take(&dp, dp.ID).RowsAffected)
	assert.Equal(t, "role", dp.AwsRole)
	assert.Equal(t, "region", dp.Region)
	assert.Equal(t, "pulumi", dp.PulumiStack)
	assert.Equal(t, "vpc", dp.VpcID)
	assert.Positive(t, dp.CreatedAt)
	assert.Positive(t, dp.UpdatedAt)
	assert.Zero(t, dp.DeletedAt)

	id := dp.ID
	assert.Positive(t, db.Delete(&dp).RowsAffected)
	assert.Zero(t, db.Take(&dp, id).RowsAffected)
	assert.Positive(t, db.Unscoped().Take(&dp, id).RowsAffected)
}
