package bridge

import (
	"context"
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUser(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	user, err := newUser(m, "test@fennel.ai", "12345")
	assert.NoError(t, err)

	assert.Equal(t, "test@fennel.ai", user.Email)
	assert.True(t, true, user.RememberCreatedAt.Valid)
	assert.True(t, true, user.RememberToken.Valid)
	assert.Equal(t, user.CreatedAt, user.RememberCreatedAt.Int64)
	assert.Equal(t, user.CreatedAt, user.UpdatedAt)
	assert.True(t, checkPasswordHash("12345", user.EncryptedPassword))

	anotherUser, err := newUser(m, "another_test@fennel.ai", "12345")
	assert.NoError(t, err)

	assert.NotEqual(t, user.EncryptedPassword, anotherUser.EncryptedPassword)
	assert.NotEqual(t, user.RememberToken.String, anotherUser.RememberToken.String)
	assert.Equal(t, len(user.RememberToken.String), len(anotherUser.RememberToken.String))
}

func TestSignInAfterSignUp(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	ctx := context.Background()

	_, err = SignIn(ctx, m, "test@fennel.ai", "12345")
	assert.ErrorIs(t, err, &ErrorUserNotFound{})
	user, err := SignUp(ctx, m, "test@fennel.ai", "12345")
	assert.NoError(t, err)

	assert.Equal(t, "test@fennel.ai", user.Email)
	assert.Positive(t, user.Id)

	_, err = SignIn(ctx, m, "test@fennel.ai", "123")
	assert.ErrorIs(t, err, &ErrorWrongPassword{})

	sameUser, err := SignIn(ctx, m, "test@fennel.ai", "12345")
	assert.NoError(t, err)

	assert.Equal(t, user.Id, sameUser.Id)
}
