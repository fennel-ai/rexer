package db

import (
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestDB(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	d := DB{
		ClusterID:            "id",
		ClusterSecurityGroup: "group",
		DBHost:               "hostname",
		AdminUsername:        "username",
		AdminPassword:        "password",
	}

	assert.Positive(t, db.Create(&d).RowsAffected)
	assert.Positive(t, d.ID)
	assert.Positive(t, db.Take(&d, d.ID).RowsAffected)

	assert.Equal(t, "id", d.ClusterID)
	assert.Equal(t, "group", d.ClusterSecurityGroup)
	assert.Equal(t, "hostname", d.DBHost)
	assert.Equal(t, "username", d.AdminUsername)
	assert.Equal(t, "password", d.AdminPassword)
	assert.Positive(t, d.CreatedAt)
	assert.Positive(t, d.UpdatedAt)
	assert.Zero(t, d.DeletedAt)

	id := d.ID
	assert.Positive(t, db.Delete(&d).RowsAffected)
	assert.Zero(t, db.Take(&d, id).RowsAffected)
	assert.Positive(t, db.Unscoped().Take(&d, id).RowsAffected)
}
