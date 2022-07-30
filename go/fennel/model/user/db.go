package user

import (
	"fennel/lib/ftypes"
	lib "fennel/lib/user"
	"fennel/mothership"
)

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

func FetchByEmail(mothership mothership.Mothership, email string) (lib.User, error) {
	user := lib.User{}
	err := mothership.DB.Get(&user, `SELECT * FROM user where email=?`, email)
	return user, err
}

func FetchByRememberToken(mothership mothership.Mothership, token string) (lib.User, error) {
	user := lib.User{}
	err := mothership.DB.Get(&user, `SELECT * FROM user where remember_token=?`, token)
	return user, err
}
