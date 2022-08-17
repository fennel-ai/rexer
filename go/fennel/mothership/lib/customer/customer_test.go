package customer

import (
	"database/sql"
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestCustomer(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	var customer Customer

	result := db.Take(&customer, "name = ?", "fennel")
	assert.Zero(t, result.RowsAffected)

	result = db.Create(&Customer{
		Name:   "fennel",
		Domain: sql.NullString{String: "fennel.ai", Valid: true},
	})
	assert.Positive(t, result.RowsAffected)

	result = db.Take(&customer, "name = ?", "fennel")
	assert.Positive(t, result.RowsAffected)

	result = db.Take(&customer, "domain = ?", "fennel.ai")
	assert.Positive(t, result.RowsAffected)

	assert.Positive(t, customer.ID)
	assert.Positive(t, customer.CreatedAt)
	assert.Positive(t, customer.UpdatedAt)
	assert.Zero(t, customer.DeletedAt)

	result = db.Delete(&customer)
	assert.NoError(t, result.Error)

	assert.Positive(t, customer.DeletedAt)
	result = db.Take(&customer, "name = ?", "fennel")
	assert.Zero(t, result.RowsAffected)
}
