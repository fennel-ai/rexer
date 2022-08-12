package user

import (
	"errors"
	"fennel/lib/ftypes"
	lib "fennel/lib/user"
	"fennel/mothership"
	"time"
)

// TODO(xiao) return or update in-place the user id
func Insert(mothership mothership.Mothership, user lib.User) (ftypes.UserId, error) {
	res, err := mothership.DB.Exec(
		`INSERT INTO user (email, encrypted_password, remember_token, remember_created_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		user.Email, user.EncryptedPassword, user.RememberToken, user.RememberCreatedAt, user.CreatedAt, user.UpdatedAt)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return ftypes.UserId(id), nil
}

func UpdateConfirmation(mothership mothership.Mothership, user lib.User) (lib.User, error) {
	if !user.IsPersisted() {
		return user, errors.New("Fail to update user, not persisted yet")
	}

	now := time.Now().UTC().UnixMicro()
	_, err := mothership.DB.Exec(
		`UPDATE user SET confirmation_token = ?, confirmed_at = ?, confirmation_sent_at = ?, updated_at = ? WHERE id = ?`,
		user.ConfirmationToken,
		user.ConfirmedAt,
		user.ConfirmationSentAt,
		now,
		user.Id,
	)
	if err != nil {
		return user, err
	}
	user.UpdatedAt = now
	return user, nil
}

func UpdateResetInfo(mothership mothership.Mothership, user lib.User) (lib.User, error) {
	if !user.IsPersisted() {
		return user, errors.New("Fail to update user, not persisted yet")
	}

	now := time.Now().UTC().UnixMicro()
	_, err := mothership.DB.Exec(
		`UPDATE user SET reset_token = ?, reset_sent_at = ?, updated_at = ? WHERE id = ?`,
		user.ResetToken,
		user.ResetSentAt,
		now,
		user.Id,
	)
	if err != nil {
		return user, err
	}
	user.UpdatedAt = now
	return user, nil
}

func FetchByEmail(mothership mothership.Mothership, email string) (lib.User, error) {
	user := lib.User{}
	err := mothership.DB.Get(&user, `SELECT * FROM user where email=?`, email)
	return user, err
}

var EmptyTokenError = errors.New("empty token")

func FetchByRememberToken(mothership mothership.Mothership, token string) (lib.User, error) {
	user := lib.User{}
	if token == "" {
		return user, EmptyTokenError
	}

	err := mothership.DB.Get(&user, `SELECT * FROM user where remember_token=?`, token)
	return user, err
}

func FetchByConfirmationToken(mothership mothership.Mothership, token string) (lib.User, error) {
	user := lib.User{}
	if token == "" {
		return user, EmptyTokenError
	}

	err := mothership.DB.Get(&user, `SELECT * FROM user where confirmation_token=?`, token)
	return user, err
}

func FetchByResetToken(mothership mothership.Mothership, token string) (lib.User, error) {
	user := lib.User{}
	if token == "" {
		return user, EmptyTokenError
	}

	err := mothership.DB.Get(&user, `SELECT * FROM user where reset_token=?`, token)
	return user, err
}
