package user

import "database/sql"

type User struct {
	Id                uint32        `db:"id"`
	Email             string        `db:"email"`
	EncryptedPassword []byte        `db:"encrypted_password"`
	DeletedAt         sql.NullInt64 `db:"deleted_at"`
	CreatedAt         int64         `db:"created_at"`
	UpdatedAt         int64         `db:"updated_at"`
}
