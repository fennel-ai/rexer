package redis

import (
	"context"
	"crypto/tls"

	"fennel/resource"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

type Client struct {
	conf   resource.Config
	client *redis.ClusterClient
	resource.Scope
}

var Nil = redis.Nil

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
	Scope     resource.Scope
}

var _ resource.Config = ClientConfig{}

func (conf ClientConfig) Materialize() (resource.Resource, error) {
	client := Client{
		conf: conf,
		client: redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:     []string{conf.Addr},
			TLSConfig: conf.TLSConfig,
		}),
		Scope: conf.Scope}
	// do a ping and verify that the client can actually talk to the server
	ctx := context.Background()
	return client, client.client.Ping(ctx).Err()
}

//=================================
// MiniRedis client config
//=================================

type MiniRedisConfig struct {
	MiniRedis *miniredis.Miniredis
	Scope     resource.Scope
}

func (conf MiniRedisConfig) Materialize() (resource.Resource, error) {
	return Client{conf, redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:     []string{conf.MiniRedis.Addr()},
		TLSConfig: nil,
	}), conf.Scope}, nil
}

var _ resource.Config = MiniRedisConfig{}
