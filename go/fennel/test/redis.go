package test

import (
	"fennel/redis"
	"fennel/resource"
	"github.com/alicebob/miniredis/v2"
)

func DefaultRedis() (resource.Resource, error) {
	mr, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	return redis.MiniRedisConfig{MiniRedis: mr}.Materialize()
}
