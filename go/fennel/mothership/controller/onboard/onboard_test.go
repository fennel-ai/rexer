package onboard

import (
	"context"
	"database/sql"
	"fennel/mothership"
	"fennel/mothership/lib/customer"
	"fennel/mothership/lib/user"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestTeamMatch(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	assert.Positive(t, db.Create(&customer.Customer{
		Name:   "Fennel",
		Domain: sql.NullString{String: "fennel.ai", Valid: true},
	}).RowsAffected)

	ctx := context.Background()

	user := user.User{
		Email: "test@fennel.AI",
	}
	matched, customer, isPersonal := TeamMatch(ctx, db, user)
	assert.True(t, matched)
	assert.False(t, isPersonal)
	assert.Equal(t, "Fennel", customer.Name)

	user.Email = "test@fennel"
	matched, _, isPersonal = TeamMatch(ctx, db, user)
	assert.False(t, matched)
	assert.False(t, isPersonal)

	user.Email = "test@Gmail.com"
	matched, _, isPersonal = TeamMatch(ctx, db, user)
	assert.False(t, matched)
	assert.True(t, isPersonal)
}
