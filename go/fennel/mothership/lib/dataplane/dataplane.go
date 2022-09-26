package dataplane

import "gorm.io/plugin/soft_delete"

type DataPlane struct {
	ID uint `gorm:"column:data_plane_id;primaryKey"`

	AwsRole               string
	Region                string
	PulumiStack           string
	VpcID                 string
	EksInstanceID         uint
	KafkaInstanceID       uint
	DBInstanceID          uint
	MemoryDBInstanceID    uint
	ElasticacheInstanceID uint
	MetricsServerAddress  string

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

type Tabler interface {
	TableName() string
}

func (DataPlane) TableName() string {
	return "data_plane"
}
