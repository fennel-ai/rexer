package kafka

import (
	"fennel/mothership"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestKafka(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: m.DB,
	}), &gorm.Config{})
	assert.NoError(t, err)

	kafka := Kafka{
		ConfluentEnvironment:  "env",
		ConfluentClusterID:    "id",
		ConfluentClusterName:  "name",
		KafkaBootstrapServers: "servers",
		KafkaApiKey:           "api",
		KafkaSecretKey:        "secret",
	}
	assert.Positive(t, db.Create(&kafka).RowsAffected)
	assert.Positive(t, kafka.ID)
	assert.Positive(t, db.Take(&kafka, kafka.ID).RowsAffected)
	assert.Equal(t, "env", kafka.ConfluentEnvironment)
	assert.Equal(t, "id", kafka.ConfluentClusterID)
	assert.Equal(t, "name", kafka.ConfluentClusterName)
	assert.Equal(t, "servers", kafka.KafkaBootstrapServers)
	assert.Equal(t, "api", kafka.KafkaApiKey)
	assert.Equal(t, "secret", kafka.KafkaSecretKey)
	assert.Positive(t, kafka.CreatedAt)
	assert.Positive(t, kafka.UpdatedAt)
	assert.Zero(t, kafka.DeletedAt)

	id := kafka.ID
	assert.Positive(t, db.Delete(&kafka).RowsAffected)
	assert.Zero(t, db.Take(&kafka, id).RowsAffected)
	assert.Positive(t, db.Unscoped().Take(&kafka, id).RowsAffected)
}
