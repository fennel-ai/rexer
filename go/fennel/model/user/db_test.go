package user

import (
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
	_, err = Insert(m, lib.User{Email: "foo@fennel.ai", EncryptedPassword: []byte("abcd")})
	assert.NoError(t, err)
	user, err := FetchByEmail(m, "foo@fennel.ai")
	assert.NoError(t, err)
	assert.Equal(t, "foo@fennel.ai", user.Email)
	assert.Equal(t, []byte("abcd"), user.EncryptedPassword)
	assert.False(t, user.DeletedAt.Valid)
	assert.Positive(t, user.CreatedAt)
	assert.Positive(t, user.UpdatedAt)
}
