package user

import (
	"database/sql"
	"fennel/mothership"
	"fennel/mothership/lib/customer"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestUser(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	var user User

	result := db.Take(&user, "email = ?", "foo@fennel.ai")
	assert.ErrorIs(t, result.Error, gorm.ErrRecordNotFound)

	result = db.Create(&User{
		Email:             "foo@fennel.ai",
		EncryptedPassword: []byte("abcd"),
	})
	assert.NoError(t, result.Error)

	result = db.Take(&user, "email = ?", "foo@fennel.ai")
	assert.NoError(t, result.Error)

	assert.Positive(t, user.ID)
	assert.Positive(t, user.CreatedAt)
	assert.Positive(t, user.UpdatedAt)
	assert.Equal(t, "foo@fennel.ai", user.Email)
	assert.Equal(t, []byte("abcd"), user.EncryptedPassword)
	assert.False(t, user.RememberToken.Valid)
	assert.False(t, user.RememberCreatedAt.Valid)
	assert.False(t, user.ConfirmationToken.Valid)
	assert.False(t, user.ConfirmationSentAt.Valid)
	assert.False(t, user.ConfirmedAt.Valid)
	assert.False(t, user.IsConfirmed())
	assert.False(t, user.ResetToken.Valid)
	assert.False(t, user.ResetSentAt.Valid)
	assert.Zero(t, user.DeletedAt)

	user.RememberToken = sql.NullString{
		String: "remember",
		Valid:  true,
	}
	user.RememberCreatedAt = sql.NullInt64{
		Int64: 12,
		Valid: true,
	}
	result = db.Save(&user)
	assert.NoError(t, result.Error)

	result = db.Model(&user).Update("ConfirmationToken", "confirmation")
	assert.NoError(t, result.Error)

	result = db.Model(&user).Updates(User{
		ConfirmationSentAt: sql.NullInt64{
			Int64: 34,
			Valid: true,
		},
	})
	assert.NoError(t, result.Error)

	customer := customer.Customer{
		Name:   "fennel",
		Domain: sql.NullString{String: "fennel.ai", Valid: true},
	}
	result = db.Create(&customer)
	assert.NoError(t, result.Error)

	result = db.Model(&user).Update("CustomerID", customer.ID)
	assert.NoError(t, result.Error)

	result = db.Joins("Customer").Take(&user, user.ID)
	assert.NoError(t, result.Error)
	assert.Equal(t, "fennel", user.Customer.Name)

	result = db.Delete(&user)
	assert.NoError(t, result.Error)
	assert.Positive(t, user.DeletedAt)

	result = db.Take(&user, "email = ?", "foo@fennel.ai")
	assert.ErrorIs(t, result.Error, gorm.ErrRecordNotFound)
	result = db.Unscoped().Take(&user, "email = ?", "foo@fennel.ai")
	assert.NoError(t, result.Error)
}
