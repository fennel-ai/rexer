package dataplane

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

	_, err = Insert(m, lib.DataPlane{
		AWSRole:               "role",
		Region:                "region",
		PulumiStack:           "pulumi",
		VPCID:                 "vpc",
		EKSInstanceID:         1,
		KafkaInstanceID:       1,
		DBInstanceID:          1,
		MemoryDBInstanceID:    1,
		ElastiCacheInstanceID: 1,
	})
	assert.NoError(t, err)
}
