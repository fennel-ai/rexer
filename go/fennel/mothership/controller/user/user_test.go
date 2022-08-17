package user

import (
	"context"
	"database/sql"
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestNewUser(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	user, err := newUser(db, "test@fennel.ai", "12345")
	assert.NoError(t, err)

	assert.Equal(t, "test@fennel.ai", user.Email)
	assert.False(t, user.RememberCreatedAt.Valid)
	assert.False(t, user.RememberToken.Valid)
	assert.Equal(t, user.CreatedAt, user.UpdatedAt)
	assert.True(t, checkPasswordHash("12345", user.EncryptedPassword))

	anotherUser, err := newUser(db, "another_test@fennel.ai", "12345")
	assert.NoError(t, err)

	assert.NotEqual(t, user.EncryptedPassword, anotherUser.EncryptedPassword)
}

func TestSignInAndLogout(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)
	ctx := context.Background()

	_, err = SignIn(ctx, db, "test@fennel.ai", "12345")
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
	user, err := SignUp(ctx, db, "test@fennel.ai", "12345")
	assert.NoError(t, err)
	assert.Equal(t, "test@fennel.ai", user.Email)
	assert.False(t, user.RememberToken.Valid)
	assert.False(t, user.RememberCreatedAt.Valid)

	_, err = SignIn(ctx, db, "test@fennel.ai", "123")
	assert.ErrorIs(t, err, ErrorWrongPassword)

	_, err = SignIn(ctx, db, "test@fennel.ai", "12345")
	assert.ErrorIs(t, err, ErrorNotConfirmed)

	user.ConfirmedAt = sql.NullInt64{Valid: true, Int64: 123}
	result := db.Model(&user).Update("ConfirmedAt", 123)
	assert.NoError(t, result.Error)

	sameUser, err := SignIn(ctx, db, "test@fennel.ai", "12345")
	assert.NoError(t, err)
	assert.Equal(t, user.ID, sameUser.ID)
	assert.True(t, sameUser.RememberToken.Valid)
	assert.True(t, sameUser.RememberCreatedAt.Valid)

	user, err = Logout(ctx, db, sameUser)
	assert.NoError(t, err)
	assert.False(t, user.RememberToken.Valid)
	assert.False(t, user.RememberCreatedAt.Valid)
}

func TestConfirmUser(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)
	ctx := context.Background()

	token := generateConfirmationToken(db)
	_, err = ConfirmUser(ctx, db, token)
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)

	user, err := SignUp(ctx, db, "test@fennel.ai", "12345")
	assert.NoError(t, err)

	result := db.Model(&user).Update("ConfirmationToken", token)
	assert.NoError(t, result.Error)

	user, err = ConfirmUser(ctx, db, token)
	assert.NoError(t, err)

	assert.True(t, user.IsConfirmed())
	assert.True(t, user.ConfirmedAt.Valid)
	assert.False(t, user.ConfirmationToken.Valid)
	assert.False(t, user.ConfirmationSentAt.Valid)
}

func TestResetPassword(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)
	ctx := context.Background()

	err = ResetPassword(ctx, db, "", "456")
	assert.ErrorIs(t, err, gorm.ErrRecordNotFound)

	user, err := SignUp(ctx, db, "test@fennel.ai", "123")
	assert.NoError(t, err)
	result := db.Model(&user).Update("ConfirmedAt", 12345)
	assert.NoError(t, result.Error)

	_, err = SignIn(ctx, db, "test@fennel.ai", "456")
	assert.Error(t, err)

	result = db.Model(&user).Update("ResetToken", "reset-oracle")
	assert.NoError(t, result.Error)

	err = ResetPassword(ctx, db, "reset-oracle", "456")
	assert.NoError(t, err)

	_, err = SignIn(ctx, db, "test@fennel.ai", "456")
	assert.NoError(t, err)
}
