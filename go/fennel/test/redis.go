package test

import (
	"fennel/lib/ftypes"
	"fennel/redis"
	"fennel/resource"
	"github.com/alicebob/miniredis/v2"
)

func mockRedis(tierID ftypes.RealmID) (redis.Client, error) {
	scope := resource.NewTierScope(tierID)
	mr, err := miniredis.Run()
	if err != nil {
		return redis.Client{}, err
	}
	rdb, err := redis.MiniRedisConfig{MiniRedis: mr, Scope: scope}.Materialize()
	if err != nil {
		return redis.Client{}, err
	}
	return rdb.(redis.Client), nil
}
