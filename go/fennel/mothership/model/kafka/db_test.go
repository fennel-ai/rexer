package kafka

import (
	"testing"

	"fennel/mothership"
	"fennel/mothership/lib"
	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	_, err = Insert(m, lib.Kafka{
		ConfluentEnvironment:  "env",
		ConfluentClusterID:    "id",
		ConfluentClusterName:  "name",
		KafkaBootstrapServers: "servers",
		KafkaAPIKey:           "api",
		KafkaSecretKey:        "secret",
	})
	assert.NoError(t, err)
}
