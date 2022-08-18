package eks

import "gorm.io/plugin/soft_delete"

type Eks struct {
	ID uint `gorm:"column:instance_id;primaryKey"`

	ClusterID    string
	MinInstances uint
	MaxInstances uint
	InstanceType string

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

type Tabler interface {
	TableName() string
}

func (Eks) TableName() string {
	return "eks"
}
