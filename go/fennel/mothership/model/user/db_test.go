package user

import (
	"database/sql"
	"testing"

	lib "fennel/lib/user"
	"fennel/mothership"

	"github.com/stretchr/testify/assert"
)

func TestFetchAfterInsert(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	_, err = FetchByEmail(m, "foo@fennel.ai")
	assert.Error(t, err)
	_, err = Insert(m, lib.User{
		Email:             "foo@fennel.ai",
		EncryptedPassword: []byte("abcd"),
		CreatedAt:         123,
	})
	assert.NoError(t, err)
	user, err := FetchByEmail(m, "foo@fennel.ai")
	assert.NoError(t, err)
	assert.Equal(t, "foo@fennel.ai", user.Email)
	assert.Equal(t, []byte("abcd"), user.EncryptedPassword)
	assert.False(t, user.DeletedAt.Valid)
	assert.False(t, user.RememberToken.Valid)
	assert.False(t, user.RememberCreatedAt.Valid)
	assert.False(t, user.ConfirmationToken.Valid)
	assert.False(t, user.ConfirmedAt.Valid)
	assert.False(t, user.ConfirmationSentAt.Valid)
	assert.Equal(t, int64(123), user.CreatedAt)
	assert.Zero(t, user.UpdatedAt)
}

func TestFetchByToken(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	user := lib.User{
		Email:             "foo@fennel.ai",
		EncryptedPassword: []byte("abcd"),
		CreatedAt:         123,
		RememberToken:     sql.NullString{String: "oracle", Valid: true},
	}
	_, err = Insert(m, user)
	assert.NoError(t, err)

	userB := lib.User{
		Email:             "bar@fennel.ai",
		EncryptedPassword: []byte("abcd"),
	}
	_, err = Insert(m, userB)
	assert.NoError(t, err)

	_, err = FetchByRememberToken(m, "")
	assert.Error(t, err)

	user, err = FetchByRememberToken(m, "oracle")
	assert.NoError(t, err)
	assert.True(t, user.IsPersisted())

	_, err = FetchByConfirmationToken(m, "")
	assert.Error(t, err)

	user.ConfirmationToken = sql.NullString{String: "confirm-oracle", Valid: true}
	user, err = UpdateConfirmation(m, user)
	assert.NoError(t, err)
	user.ConfirmationSentAt = sql.NullInt64{Int64: 456, Valid: true}
	user.ConfirmedAt = sql.NullInt64{Int64: 789, Valid: true}
	user, err = UpdateConfirmation(m, user)
	assert.NoError(t, err)

	user, err = FetchByConfirmationToken(m, "confirm-oracle")
	assert.NoError(t, err)

	assert.True(t, user.ConfirmationSentAt.Valid)
	assert.Equal(t, int64(456), user.ConfirmationSentAt.Int64)
	assert.True(t, user.ConfirmedAt.Valid)
	assert.Equal(t, int64(789), user.ConfirmedAt.Int64)
	assert.True(t, user.IsConfirmed())

	_, err = FetchByResetToken(m, "")
	assert.Error(t, err)

	user.ResetToken = sql.NullString{String: "reset-oracle", Valid: true}
	user, err = UpdateResetInfo(m, user)
	assert.NoError(t, err)
	user.ResetSentAt = sql.NullInt64{Int64: 1234, Valid: true}
	user, err = UpdateResetInfo(m, user)
	assert.NoError(t, err)

	user, err = FetchByResetToken(m, "reset-oracle")
	assert.NoError(t, err)
	assert.Equal(t, int64(1234), user.ResetSentAt.Int64)
}

func TestUpdatePassword(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	user := lib.User{
		Email:             "foo@fennel.ai",
		EncryptedPassword: []byte("abcd"),
	}
	user_id, err := Insert(m, user)
	assert.NoError(t, err)

	user.Id = user_id
	user.EncryptedPassword = []byte("efgh")
	user.ResetToken = sql.NullString{Valid: true, String: "x"}
	user.ResetSentAt = sql.NullInt64{Valid: true, Int64: 123}
	_, err = UpdatePassword(m, user)
	assert.NoError(t, err)

	user, err = FetchByEmail(m, "foo@fennel.ai")
	assert.NoError(t, err)

	assert.Equal(t, []byte("efgh"), user.EncryptedPassword)
	assert.True(t, user.ResetToken.Valid)
	assert.Equal(t, "x", user.ResetToken.String)
	assert.True(t, user.ResetSentAt.Valid)
	assert.Equal(t, int64(123), user.ResetSentAt.Int64)
}
