package elasticache

import "gorm.io/plugin/soft_delete"

type ElastiCache struct {
	ID uint `gorm:"column:instance_id;primaryKey"`

	ClusterID            string
	ClusterSecurityGroup string
	PrimaryHostname      string
	ReplicaHostname      string

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

type Tabler interface {
	TableName() string
}

func (ElastiCache) TableName() string {
	return "elasticache"
}
