package bootarg

import (
	"fennel/db"
	instance2 "fennel/instance"
	"fennel/redis"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Create_GetInstance(t *testing.T) {
	instance := instance2.Instance{
		CustID:         123,
		Type:           instance2.TEST,
		Name:           "myinstance",
		DB:             db.Connection{},
		Redis:          redis.Client{},
		Cache:          nil,
		ActionProducer: nil,
		ActionConsumer: nil,
		Clock:          &test.FakeClock{},
	}
	b := Create(instance)
	assert.Len(t, b, 1)
	found, err := GetInstance(b)
	assert.NoError(t, err)
	assert.Equal(t, instance, found)
}
