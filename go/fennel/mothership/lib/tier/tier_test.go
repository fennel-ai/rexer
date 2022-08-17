package tier

import (
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestTier(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	tier := Tier{
		DataPlaneID:  1,
		CustomerID:   2,
		PulumiStack:  "pulumi",
		ApiUrl:       "url",
		K8sNamespace: "namespace",
	}

	result := db.Create(&tier)
	assert.Positive(t, result.RowsAffected)
	assert.Positive(t, tier.ID)
	assert.Positive(t, tier.CreatedAt)
	assert.Positive(t, tier.UpdatedAt)
	assert.Zero(t, tier.DeletedAt)

	result = db.Take(&tier, "customer_id = ?", 2)
	assert.Positive(t, result.RowsAffected)

	result = db.Delete(&tier)
	assert.Positive(t, result.RowsAffected)

	result = db.Take(&tier, "customer_id = ?", 2)
	assert.Zero(t, result.RowsAffected)
	result = db.Unscoped().Take(&tier, "customer_id = ?", 2)
	assert.Positive(t, result.RowsAffected)
}
