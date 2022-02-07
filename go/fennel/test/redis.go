package test

import (
	"fennel/lib/ftypes"
	"fennel/redis"
	"github.com/alicebob/miniredis/v2"
)

func mockRedis(tierID ftypes.TierID) (redis.Client, error) {
	mr, err := miniredis.Run()
	if err != nil {
		return redis.Client{}, err
	}
	rdb, err := redis.MiniRedisConfig{MiniRedis: mr}.Materialize(tierID)
	if err != nil {
		return redis.Client{}, err
	}
	return rdb.(redis.Client), nil
}
