package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"fennel/lib/timer"

	"github.com/go-redis/redis/v8"
)

func (c Client) Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error {
	defer timer.Start(ctx, c.ID(), "redis.set").Stop()
	k = c.tieredKey(k)
	return c.client.Set(ctx, k, v, ttl).Err()
}

func (c Client) SetNX(ctx context.Context, key string, v interface{}, ttl time.Duration) (bool, error) {
	defer timer.Start(ctx, c.ID(), "redis.setnx").Stop()
	key = c.tieredKey(key)
	return c.client.SetNX(ctx, key, v, ttl).Result()
}

func (c Client) Del(ctx context.Context, k ...string) error {
	defer timer.Start(ctx, c.ID(), "redis.del").Stop()
	pipe := c.client.Pipeline()
	for _, key := range k {
		if err := pipe.Del(ctx, c.tieredKey(key)).Err(); err != nil {
			return err
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c Client) Get(ctx context.Context, k string) (interface{}, error) {
	defer timer.Start(ctx, c.ID(), "redis.get").Stop()
	k = c.tieredKey(k)
	return c.client.Get(ctx, k).Result()
}

// MGet takes a list of strings and returns a list of interfaces along with an error
// returned is either the correct value of key or redis.Nil
func (c Client) MGet(ctx context.Context, ks ...string) ([]interface{}, error) {
	defer timer.Start(ctx, c.ID(), "redis.mget").Stop()
	// this check is to handle a bug, likely related to https://github.com/redis/node-redis/issues/125
	if len(ks) == 0 {
		return []interface{}{}, nil
	}

	pipe := c.client.Pipeline()
	results := make([]*redis.StringCmd, len(ks))
	for i := range ks {
		results[i] = pipe.Get(ctx, c.tieredKey(ks[i]))
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	vs := make([]interface{}, len(ks))
	for i := range results {
		res, err := results[i].Result()
		switch err {
		case nil:
			vs[i] = res
		case redis.Nil:
			vs[i] = redis.Nil
		default:
			return nil, err
		}
	}
	return vs, nil
}

// Same as MGet without transforming the keys
func (c Client) MRawGet(ctx context.Context, ks ...string) ([]interface{}, error) {
	defer timer.Start(ctx, c.ID(), "redis.mget").Stop()
	// this check is to handle a bug, likely related to https://github.com/redis/node-redis/issues/125
	if len(ks) == 0 {
		return []interface{}{}, nil
	}

	pipe := c.client.Pipeline()
	results := make([]*redis.StringCmd, len(ks))
	for i := range ks {
		results[i] = pipe.Get(ctx, ks[i])
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	vs := make([]interface{}, len(ks))
	for i := range results {
		res, err := results[i].Result()
		switch err {
		case nil:
			vs[i] = res
		case redis.Nil:
			vs[i] = redis.Nil
		default:
			return nil, err
		}
	}
	return vs, nil
}

func (c Client) MSet(ctx context.Context, keys []string, values []interface{}, ttls []time.Duration) error {
	defer timer.Start(ctx, c.ID(), "redis.mset").Stop()
	// nothing to write if there are no keys.
	if len(keys) == 0 {
		return nil
	}
	if len(keys) != len(values) || len(keys) != len(ttls) {
		return fmt.Errorf("keys, values, and ttls should all be slices of the same length")
	}
	pipe := c.client.Pipeline()
	for i, key := range keys {
		pipe.Set(ctx, c.tieredKey(key), values[i], ttls[i])
	}
	_, err := pipe.Exec(ctx)
	return err
}

// SetNXPipelined pipelines and executes multiple SetNX commands and returns their result as a list of bools
// Returns any error before execution but ignores errors that happen during execution
func (c Client) SetNXPipelined(ctx context.Context, keys []string, values []interface{}, ttls []time.Duration) (
	ok []bool, err error) {
	defer timer.Start(ctx, c.ID(), "redis.setnx_pipelined").Stop()
	// nothing to write if there are no keys.
	if len(keys) == 0 {
		return nil, nil
	}
	if len(keys) != len(values) || len(keys) != len(ttls) {
		return nil, fmt.Errorf("keys, values, and ttls should all be slices of the same length")
	}
	pipe := c.client.Pipeline()
	for i, key := range keys {
		err = pipe.SetNX(ctx, c.tieredKey(key), values[i], ttls[i]).Err()
		// Return errors that happen before execution
		if err != nil {
			return nil, err
		}
	}
	cmds, _ := pipe.Exec(ctx)
	ok = make([]bool, len(keys))
	for i, cmd := range cmds {
		ok[i], err = cmd.(*redis.BoolCmd).Result()
		// Log errors that happened during execution
		if err != nil {
			log.Printf("Redis Error: SetNXPipelined(): %v", err)
		}
	}
	return ok, nil
}

func (c Client) tieredKey(k string) string {
	return c.PrefixedName(k)
}

func (c Client) mTieredKey(ks []string) []string {
	ret := make([]string, len(ks))
	for i, k := range ks {
		ret[i] = c.tieredKey(k)
	}
	return ret
}
