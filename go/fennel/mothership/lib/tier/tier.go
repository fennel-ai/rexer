package tier

import (
	"fennel/mothership/lib/dataplane"
	"strconv"

	"gorm.io/plugin/soft_delete"
)

type Tier struct {
	ID uint `gorm:"column:tier_id;primaryKey"`

	PulumiStack   string
	ApiUrl        string
	K8sNamespace  string
	RequestsLimit uint
	Plan          uint

	CustomerID uint

	DataPlaneID uint
	DataPlane   dataplane.DataPlane

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

const (
	TierPlanPersonal = iota
	TierPlanStartup
	TierPlanEnterprise
)

func (t *Tier) PlanName() string {
	switch t.Plan {
	case TierPlanPersonal:
		return "Personal"
	case TierPlanStartup:
		return "Startup"
	case TierPlanEnterprise:
		return "Enterprise"
	}
	return "Unknown"
}

func (t *Tier) IDStr() string {
	return strconv.FormatUint(uint64(t.ID), 10)
}

type Tabler interface {
	TableName() string
}

func (Tier) TableName() string {
	return "tier"
}
