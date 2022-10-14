package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.uber.org/zap"

	"fennel/lib/timer"

	"github.com/go-redis/redis/v8"
)

type SetReturnType int32

const (
	NotFoundSet SetReturnType = 0
	FoundSkip SetReturnType = 1
	Error SetReturnType = 2
)

func (c Client) Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error {
	ctx, t := timer.Start(ctx, c.ID(), "redis.set")
	defer t.Stop()

	k = c.tieredKey(k)
	return c.client.Set(ctx, k, v, ttl).Err()
}

func (c Client) SetNX(ctx context.Context, key string, v interface{}, ttl time.Duration) (bool, error) {
	ctx, t := timer.Start(ctx, c.ID(), "redis.setnx")
	defer t.Stop()

	key = c.tieredKey(key)
	return c.client.SetNX(ctx, key, v, ttl).Result()
}

func (c Client) Del(ctx context.Context, k ...string) error {
	ctx, t := timer.Start(ctx, c.ID(), "redis.del")
	defer t.Stop()

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
	ctx, t := timer.Start(ctx, c.ID(), "redis.get")
	defer t.Stop()

	k = c.tieredKey(k)
	return c.client.Get(ctx, k).Result()
}

// MGet takes a list of strings and returns a list of interfaces along with an error
// returned is either the correct value of key or redis.Nil
func (c Client) MGet(ctx context.Context, ks ...string) ([]interface{}, error) {
	ctx, t := timer.Start(ctx, c.ID(), "redis.mget")
	defer t.Stop()

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
	ctx, t := timer.Start(ctx, c.ID(), "redis.mget")
	defer t.Stop()

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
	ctx, t := timer.Start(ctx, c.ID(), "redis.mset")
	defer t.Stop()

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

// SetNXPipelined pipelines and executes multiple SetNX commands. Returns:
// 	1. Found - if a key already exists, so the new value was not set
// 	2. NotFound - key previously did not exist, new value was set
//  3. Error - Set command failed
func (c Client) SetNXPipelined(
	ctx context.Context, keys []string, values []interface{}, ttls []time.Duration,
) (ok []SetReturnType, err error) {
	ctx, t := timer.Start(ctx, c.ID(), "redis.setnx_pipelined")
	defer t.Stop()

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
	ok = make([]SetReturnType, len(keys))
	for i, cmd := range cmds {
		set, err := cmd.(*redis.BoolCmd).Result()
		// Log errors that happened during execution
		if err != nil {
			zap.L().Warn("Redis Error: SetNXPipelined()", zap.Error(err))
			ok[i] = Error
		} else {
			if set {
				ok[i] = NotFoundSet
			} else {
				ok[i] = FoundSkip
			}
		}
	}
	return ok, nil
}

func (c Client) HGetAllPipelined(ctx context.Context, keys ...string) (hmaps []map[string]string, err error) {
	ctx, t := timer.Start(ctx, c.ID(), "redis.hgetall_pipelined")
	defer t.Stop()

	// nothing to get if there are no keys
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := c.client.Pipeline()
	for _, key := range keys {
		_, err := pipe.HGetAll(ctx, c.tieredKey(key)).Result()
		// Return errors that happen before execution
		if err != nil {
			return nil, err
		}
	}
	cmds, _ := pipe.Exec(ctx)
	hmaps = make([]map[string]string, len(keys))
	for i, cmd := range cmds {
		hmaps[i], err = cmd.(*redis.StringStringMapCmd).Result()
		// Log errors that happened during execution
		if err != nil {
			log.Printf("Redis Error: HGetAllPipelined(): %v", err)
		}
	}
	return hmaps, nil
}

// HSetPipelined sets the hashes with the provided values. TTL is set only if it is non-zero.
// Since ExpireNX does not work, when it is not desired to update the TTL, it should be zero
// and provide a TTL when first setting the hashmap instead. When a hashmap has no associated
// TTL then it does not expire.
func (c Client) HSetPipelined(
	ctx context.Context, keys []string, values []map[string]interface{}, ttls []time.Duration,
) (err error) {
	ctx, t := timer.Start(ctx, c.ID(), "redis.hset_pipelined")
	defer t.Stop()

	// nothing to set if there are no keys
	if len(keys) == 0 {
		return nil
	}
	if len(keys) != len(values) {
		return fmt.Errorf("keys and values should have the same length")
	}
	pipe := c.client.Pipeline()
	for i, key := range keys {
		// redis gives an error sometimes when trying to set an empty map
		// so we skip it
		if len(values[i]) == 0 {
			continue
		}
		err = pipe.HSet(ctx, c.tieredKey(key), values[i]).Err()
		if err != nil {
			// Return errors that happen before execution
			return err
		}
		if ttls[i] != 0 {
			// does not set TTL when it is 0
			err = pipe.Expire(ctx, c.tieredKey(key), ttls[i]).Err()
			if err != nil {
				return err
			}
		}
	}
	cmds, _ := pipe.Exec(ctx)
	for i, cmd := range cmds {
		err = cmd.Err()
		// Log errors that happened during execution
		if err != nil {
			log.Printf("Redis Error: HSetPipelined()[%d]: %v", i, err)
		}
	}
	return err
}

func (c Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.client.TTL(ctx, key).Result()
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
