package redis

import (
	"crypto/tls"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	addr = "clustercfg.redis-db-e5ae558.sumkzb.memorydb.ap-south-1.amazonaws.com:6379"
)

func TestRedisClientIntegration(t *testing.T) {
	// TODO: verify this test passes
	t.SkipNow()
	conf := ClientConfig{Addr: addr, TLSConfig: &tls.Config{}}
	rdb, err := conf.Materialize()
	assert.NoError(t, err)
	testClient(t, rdb.(Client))
}
