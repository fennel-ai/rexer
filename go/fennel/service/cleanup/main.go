package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/lib/utils/encoding/base91"
	"fennel/model/aggregate"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/go-redis/redis/v8"
	"golang.org/x/net/context"
)

type DeleteCounterArgs struct {
	BatchSize  int      `arg:"--batch_size,env:BATCH_SIZE" default:"10000" json:"batch_size,omitempty"`
	Aggregates []uint32 `arg:"--aggregates,env:AGGREGATES" json:"aggregates,omitempty"`
}

func redisKeyPrefix(tr tier.Tier, aggId ftypes.AggId) (string, error) {
	aggBuf := make([]byte, 8) // aggId
	curr, err := binary.PutUvarint(aggBuf, uint64(aggId))
	if err != nil {
		return "", err
	}
	aggStr := base91.StdEncoding.Encode(aggBuf[:curr])
	// TODO(mohit): redis key delimiter is hardcode, consider unifying this by making it a lib
	return fmt.Sprintf("%s-*", tr.Redis.Scope.PrefixedName(aggStr)), nil
}

func deleteKeys(tr tier.Tier, aggId ftypes.AggId, rdb *redis.ClusterClient, batchSize int) error {
	// fetch and delete batch of keys
	keyPrefix, err := redisKeyPrefix(tr, aggId)
	if err != nil {
		return err
	}
	tr.Logger.Info(fmt.Sprintf("[%d] deleting keys with prefix: %s\n", aggId, keyPrefix))

	var cursor uint64
	var n int
	for {
		ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

		var keys []string
		var err error
		oldC := cursor
		keys, cursor, err = rdb.Scan(ctx, cursor, keyPrefix, int64(batchSize)).Result()
		if err != nil {
			return err
		}

		// delete the keys found in the scan above
		p := rdb.Pipeline()
		for _, k := range keys {
			// using `Unlink`` here instead of Del to avoid a blocking call to redis
			// Unlink removes the key from the keyspace but the memory is reclaimed by redis in the background
			if err := p.Unlink(ctx, k).Err(); err != nil {
				return err
			}
		}
		if _, err := p.Exec(ctx); err != nil {
			return err
		}

		// log basic stats
		n += len(keys)
		tr.Logger.Info(fmt.Sprintf("[%d] [cursor] %d -> %d; [keys] found: %d, total so far: %d\n", aggId, oldC, cursor, len(keys), n))
		if cursor == 0 {
			break
		}
	}
	return nil
}

func main() {
	// seed random number generator so that all uses of rand work well
	rand.Seed(time.Now().UnixNano())
	var flags struct {
		tier.TierArgs
		DeleteCounterArgs
	}
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	tier, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup tier connectors: %v", err))
	}

	rdb := tier.Redis.Client().(*redis.ClusterClient)

	if len(flags.Aggregates) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(len(flags.Aggregates))
		for _, aggId := range flags.Aggregates {
			go func(aggId uint32) {
				defer wg.Done()
				if err := deleteKeys(tier, ftypes.AggId(aggId), rdb, flags.BatchSize); err != nil {
					tier.Logger.Info(fmt.Sprintf("redis key deletion for aggId: %d, failed with: %v", aggId, err))
				}
			}(aggId)
		}
		wg.Wait()
	} else {
		tier.Logger.Info("--aggregates is not set, will delete keys for all inactive aggregates\n")
		aggs, err := aggregate.RetrieveAll(context.Background(), tier)
		if err != nil {
			panic(err)
		}
		wg := sync.WaitGroup{}
		for _, agg := range aggs {
			if !agg.Active {
				wg.Add(1)
				go func(aggId ftypes.AggId) {
					defer wg.Done()
					if err := deleteKeys(tier, aggId, rdb, flags.BatchSize); err != nil {
						tier.Logger.Info(fmt.Sprintf("redis key deletion for aggId: %d, failed with: %v", aggId, err))
					}
				}(agg.Id)
			}
		}
		wg.Wait()
	}
}
