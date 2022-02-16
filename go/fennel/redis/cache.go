package redis

import (
	"context"
	"fennel/lib/cache"
	"github.com/go-redis/redis/v8"
	"time"
)

type Cache struct {
	client Client
}

func (c Cache) MGet(ctx context.Context, k ...string) ([]interface{}, error) {
	return c.client.MGet(ctx, k...)
}

func (c Cache) MSet(ctx context.Context, ks []string, vs []interface{}, ttls []time.Duration) error {
	return c.client.MSet(ctx, ks, vs, ttls)
}

func (c Cache) Nil() error {
	return redis.Nil
}

var _ cache.Cache = Cache{}

func NewCache(client Client) Cache {
	return Cache{client: client}
}

func (c Cache) Get(ctx context.Context, k string) (interface{}, error) {
	return c.client.Get(ctx, k)
}

func (c Cache) Delete(ctx context.Context, k ...string) error {
	return c.client.Del(ctx, k...)
}

func (c Cache) Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error {
	return c.client.Set(ctx, k, v, ttl)
}

func (c Cache) Init() error {
	return nil
}
