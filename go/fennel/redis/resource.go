package redis

import (
	"context"
	"crypto/tls"
	"fmt"

	"fennel/resource"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

// TODO: Consider adding tags for `client` and validating that it can either be a
// `redis.Client` or `redis.Tx`
type Client struct {
	conf   resource.Config
	client redis.Cmdable
	resource.Scope
}

var Nil = redis.Nil

func (c Client) Client() redis.Cmdable {
	return c.client
}

func (c Client) Type() resource.Type { return resource.RedisClient }

func (c Client) Close() error {
	if client, ok := (c.client).(*redis.ClusterClient); ok {
		if err := client.Close(); err != nil {
			return err
		}
	}
	if client, ok := (c.client).(*redis.Tx); ok {
		if err := client.Close(client.Context()); err != nil {
			return err
		}
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
	fmt.Println("Seting up redis client", conf.Addr)
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
