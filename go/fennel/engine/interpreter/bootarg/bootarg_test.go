package bootarg

import (
	"testing"

	"github.com/raulk/clock"

	"fennel/db"
	"fennel/redis"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
)

func Test_Create_GetInstance(t *testing.T) {
	tier := tier.Tier{
		DB:               db.Connection{},
		Redis:            redis.Client{},
		Cache:            nil,
		Producers:        nil,
		NewKafkaConsumer: nil,
		Clock:            clock.NewMock(),
	}
	b := Create(tier)
	assert.Len(t, b, 1)

	found1, err := GetTier(b)
	assert.NoError(t, err)
	assert.Equal(t, tier, found1)
}
