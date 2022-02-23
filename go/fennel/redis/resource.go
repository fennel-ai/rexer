package redis

import (
	"context"
	"crypto/tls"

	"fennel/lib/ftypes"
	"fennel/resource"
	"fmt"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

type Client struct {
	tierID ftypes.TierID
	conf   resource.Config
	client *redis.Client
}

var Nil = redis.Nil

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

func (conf ClientConfig) Materialize(scope resource.Scope) (resource.Resource, error) {
	if scope.GetTierID() == 0 {
		return nil, fmt.Errorf("tier ID not valid")
	}
	client := Client{scope.GetTierID(), conf, redis.NewClient(&redis.Options{
		Addr:      conf.Addr,
		TLSConfig: conf.TLSConfig,
	})}
	// do a ping and verify that the client can actually talk to the server
	ctx := context.Background()
	return client, client.client.Ping(ctx).Err()
}

//=================================
// MiniRedis client config
//=================================

type MiniRedisConfig struct {
	MiniRedis *miniredis.Miniredis
}

func (conf MiniRedisConfig) Materialize(scope resource.Scope) (resource.Resource, error) {
	if scope.GetTierID() == 0 {
		return nil, fmt.Errorf("tier ID not valid")
	}
	return Client{scope.GetTierID(), conf, redis.NewClient(&redis.Options{
		Addr:      conf.MiniRedis.Addr(),
		TLSConfig: nil,
	})}, nil
}

var _ resource.Config = MiniRedisConfig{}
