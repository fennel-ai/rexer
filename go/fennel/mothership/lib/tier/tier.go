package tier

import (
	"fennel/mothership/lib/dataplane"

	"gorm.io/plugin/soft_delete"
)

type Tier struct {
	ID uint `gorm:"column:tier_id;primaryKey"`

	PulumiStack   string
	ApiUrl        string
	K8sNamespace  string
	RequestsLimit uint

	CustomerID uint

	DataPlaneID uint
	DataPlane   dataplane.DataPlane

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
