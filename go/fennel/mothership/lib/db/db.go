package db

import "gorm.io/plugin/soft_delete"

type DB struct {
	ID uint `gorm:"column:instance_id;primaryKey"`

	ClusterID            string
	ClusterSecurityGroup string
	DBHost               string
	AdminUsername        string
	AdminPassword        string

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

type Tabler interface {
	TableName() string
}

func (DB) TableName() string {
	return "db"
}
