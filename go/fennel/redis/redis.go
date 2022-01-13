package redis

import (
	"crypto/tls"

	"github.com/go-redis/redis/v8"
)

func NewClient(addr string, tlsConfig *tls.Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:      addr,
		TLSConfig: tlsConfig,
	})
}
