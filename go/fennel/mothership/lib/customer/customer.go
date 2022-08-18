package customer

import (
	"database/sql"
	"fennel/mothership/lib/tier"

	"gorm.io/plugin/soft_delete"
)

type Customer struct {
	ID     uint `gorm:"column:customer_id;primaryKey"`
	Name   string
	Domain sql.NullString
	Tiers  []tier.Tier

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

type Tabler interface {
	TableName() string
}

func (Customer) TableName() string {
	return "customer"
}
