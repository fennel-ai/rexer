package bootarg

import (
	"fennel/db"
	"fennel/redis"
	"fennel/test"
	"fennel/tier"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Create_GetInstance(t *testing.T) {
	tier := tier.Tier{
		CustID:    123,
		DB:        db.Connection{},
		Redis:     redis.Client{},
		Cache:     nil,
		Producers: nil,
		Consumers: nil,
		Clock:     &test.FakeClock{},
	}
	b := Create(tier)
	assert.Len(t, b, 1)
	found, err := GetTier(b)
	assert.NoError(t, err)
	assert.Equal(t, tier, found)
}
