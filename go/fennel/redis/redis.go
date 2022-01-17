package redis

import (
	"crypto/tls"
	"fennel/instance"
	"fennel/resource"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

type Client struct {
	conf resource.Config
	*redis.Client
}

func (c Client) Teardown() error { return nil }

func (c Client) Type() resource.Type { return resource.RedisClient }

func (c Client) Close() error {
	err := c.Client.Close()
	if err != nil {
		return err
	}
	if conf, ok := c.conf.(MiniRedisConfig); ok {
		conf.mr.Close()
	}
	return nil
}

var _ resource.Resource = Client{}

func DefaultClient() (resource.Resource, error) {
	switch instance.Current() {
	case instance.PROD:
		return defaultConfig.Materialize()
	case instance.TEST:
		mr, err := miniredis.Run()
		if err != nil {
			return nil, err
		}
		return MiniRedisConfig{mr}.Materialize()
	default:
		return nil, fmt.Errorf("unsupported instance: %v", instance.Current())
	}
}

//=================================
// Redis client config
//=================================

type ClientConfig struct {
	Addr      string
	TLSConfig *tls.Config
}

var _ resource.Config = ClientConfig{}

func (conf ClientConfig) Materialize() (resource.Resource, error) {
	return Client{conf, redis.NewClient(&redis.Options{
		Addr:      conf.Addr,
		TLSConfig: conf.TLSConfig,
	})}, nil
}

var defaultConfig = ClientConfig{
	Addr:      "clustercfg.redis-db-e5ae558.sumkzb.memorydb.ap-south-1.amazonaws.com:6379",
	TLSConfig: &tls.Config{},
}

//=================================
// MiniRedis client config
//=================================

type MiniRedisConfig struct {
	mr *miniredis.Miniredis
}

func (conf MiniRedisConfig) Materialize() (resource.Resource, error) {
	return Client{conf, redis.NewClient(&redis.Options{
		Addr:      conf.mr.Addr(),
		TLSConfig: nil,
	})}, nil
}

var _ resource.Config = MiniRedisConfig{}
