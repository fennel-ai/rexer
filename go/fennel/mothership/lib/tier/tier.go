package tier

import "gorm.io/plugin/soft_delete"

type Tier struct {
	ID uint `gorm:"column:tier_id;primaryKey"`

	PulumiStack  string
	ApiUrl       string
	K8sNamespace string

	DataPlaneID uint
	CustomerID  uint

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

type Tabler interface {
	TableName() string
}

func (Tier) TableName() string {
	return "tier"
}
