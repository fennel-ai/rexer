package user

import (
	"database/sql"
	"fennel/lib/ftypes"
	"fennel/mothership/lib/customer"

	"gorm.io/plugin/soft_delete"
)

type User struct {
	ID                 ftypes.UserId
	Email              string
	EncryptedPassword  []byte
	RememberToken      sql.NullString
	RememberCreatedAt  sql.NullInt64
	ConfirmationToken  sql.NullString
	ConfirmationSentAt sql.NullInt64
	ConfirmedAt        sql.NullInt64
	ResetToken         sql.NullString
	ResetSentAt        sql.NullInt64
	CustomerID         uint
	Customer           customer.Customer

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

func (u *User) IsConfirmed() bool {
	return u.ConfirmedAt.Valid
}

type Tabler interface {
	TableName() string
}

func (User) TableName() string {
	return "user"
}
