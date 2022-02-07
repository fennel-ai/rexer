package test

import (
	"crypto/tls"
	"fennel/lib/ftypes"
	"fennel/redis"
	"github.com/alicebob/miniredis/v2"
)

const addr = "clustercfg.redis-db-5dec5dd.fbjfph.memorydb.us-west-2.amazonaws.com:6379"

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

func integrationRedis(tierID ftypes.TierID) (redis.Client, error) {
	conf := redis.ClientConfig{Addr: addr, TLSConfig: &tls.Config{}}
	rdb, err := conf.Materialize(tierID)
	if err != nil {
		return redis.Client{}, err
	}
	return rdb.(redis.Client), nil
}
