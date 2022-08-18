package kafka

import "gorm.io/plugin/soft_delete"

type Kafka struct {
	ID uint `gorm:"column:instance_id;primaryKey"`

	ConfluentEnvironment string
	ConfluentClusterID   string
	ConfluentClusterName string

	KafkaBootstrapServers string
	KafkaApiKey           string
	KafkaSecretKey        string

	DeletedAt soft_delete.DeletedAt `gorm:"softDelete:milli"`
	CreatedAt int64                 `gorm:"autoUpdateTime:milli"`
	UpdatedAt int64                 `gorm:"autoUpdateTime:milli"`
}

type Tabler interface {
	TableName() string
}

func (Kafka) TableName() string {
	return "kafka"
}
