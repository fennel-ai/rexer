package memory_db

import "gorm.io/plugin/soft_delete"

type MemoryDB struct {
	ID uint `gorm:"column:instance_id;primaryKey"`

	ClusterID            string
	ClusterSecurityGroup string
	Hostname             string

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

type Tabler interface {
	TableName() string
}

func (MemoryDB) TableName() string {
	return "memory_db"
}
