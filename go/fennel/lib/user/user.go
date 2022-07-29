package user

import "database/sql"

type User struct {
	Id                uint32         `db:"id"`
	Email             string         `db:"email"`
	EncryptedPassword []byte         `db:"encrypted_password"`
	RememberToken     sql.NullString `db:"remember_token"`
	RememberCreatedAt sql.NullInt64  `db:"remember_created_at"`
	DeletedAt         sql.NullInt64  `db:"deleted_at"`
	CreatedAt         int64          `db:"created_at"`
	UpdatedAt         int64          `db:"updated_at"`
}
