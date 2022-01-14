package redis

import (
	"context"
	"fennel/cache"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

type Cache struct {
	client *redis.Client
}

func (c Cache) Nil() error {
	return redis.Nil
}

var _ cache.Cache = Cache{}

func NewCache(client *redis.Client) Cache {
	return Cache{client: client}
}

func (c Cache) Get(ctx context.Context, k string) (interface{}, error) {
	return c.client.Get(ctx, k).Result()
}

func (c Cache) Delete(ctx context.Context, k string) error {
	return c.client.Del(ctx, k).Err()
}

func (c Cache) Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error {
	return c.client.Set(ctx, k, v, ttl).Err()
}

func (c Cache) Init() error {
	if c.client == nil {
		return fmt.Errorf("client can not be nil")
	} else {
		return nil
	}
}
