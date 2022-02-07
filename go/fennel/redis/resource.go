package redis

import (
	"crypto/tls"
	"fennel/lib/ftypes"
	"fennel/resource"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

type Client struct {
	tierID ftypes.TierID
	conf   resource.Config
	client *redis.Client
}

func (c Client) TierID() ftypes.TierID {
	return c.tierID
}

func (c Client) Type() resource.Type { return resource.RedisClient }

func (c Client) Close() error {
	err := c.client.Close()
	if err != nil {
		return err
	}
	if conf, ok := c.conf.(MiniRedisConfig); ok {
		conf.MiniRedis.Close()
	}
	return nil
}

var _ resource.Resource = Client{}

//=================================
// Redis client config
//=================================

type ClientConfig struct {
	Addr      string
	TLSConfig *tls.Config
}

var _ resource.Config = ClientConfig{}

func (conf ClientConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	return Client{tierID, conf, redis.NewClient(&redis.Options{
		Addr:      conf.Addr,
		TLSConfig: conf.TLSConfig,
	})}, nil
}

//=================================
// MiniRedis client config
//=================================

type MiniRedisConfig struct {
	MiniRedis *miniredis.Miniredis
}

func (conf MiniRedisConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	return Client{tierID, conf, redis.NewClient(&redis.Options{
		Addr:      conf.MiniRedis.Addr(),
		TLSConfig: nil,
	})}, nil
}

var _ resource.Config = MiniRedisConfig{}
