package user

import (
	lib "fennel/lib/user"
	"fennel/mothership"
	"time"
)

func Insert(mothership mothership.Mothership, user lib.User) (uint32, error) {
	now := time.Now().UTC().UnixMicro()
	created_at := now
	if user.CreatedAt > 0 {
		created_at = user.CreatedAt
	}
	updated_at := now
	if user.UpdatedAt > 0 {
		updated_at = user.UpdatedAt
	}
	res, err := mothership.DB.Exec(
		`INSERT INTO user (email, encrypted_password, created_at, updated_at) VALUES (?, ?, ?, ?)`,
		user.Email, user.EncryptedPassword, created_at, updated_at)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}

func FetchByEmail(mothership mothership.Mothership, email string) (lib.User, error) {
	user := lib.User{}
	err := mothership.DB.Get(&user, `SELECT * FROM user where email=?`, email)
	return user, err
}
