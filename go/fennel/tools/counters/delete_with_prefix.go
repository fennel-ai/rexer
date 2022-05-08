package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/go-redis/redis/v8"
	"golang.org/x/net/context"
)

type DeleteCounterArgs struct {
	Addr      string `arg:"--addr,env:ADDR" json:"addr,omitempty"`
	BatchSize int    `arg:"--batch_size,env:BATCH_SIZE" default:"100000" json:"batch_size,omitempty"`
	Prefix    string `arg:"--prefix,env:PREFIX" json:"prefix,omitempty"`
}

func main() {
	var flags struct {
		DeleteCounterArgs
	}
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(flags.Addr) == 0 {
		panic(fmt.Errorf("--addr should be set to non-empty memorydb address"))
	}

	if len(flags.Prefix) == 0 {
		panic(fmt.Errorf("--prefix should be set to non-empty redis key prefix"))
	}

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:     []string{flags.Addr},
		TLSConfig: &tls.Config{},
	})

	// fetch and delete batch of keys
	var cursor uint64
	var n int
	for {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

		var keys []string
		var err error
		fmt.Printf("============\n")
		fmt.Printf("[cursor]: %d ", cursor)
		keys, cursor, err = rdb.Scan(ctx, cursor, flags.Prefix, int64(flags.BatchSize)).Result()
		if err != nil {
			panic(err)
		}

		// delete the keys found in the scan above
		p := rdb.Pipeline()
		for _, k := range keys {
			if err := p.Unlink(ctx, k).Err(); err != nil {
				panic(err)
			}
		}
		if _, err := p.Exec(ctx); err != nil {
			panic(err)
		}

		// log basic stats
		n += len(keys)
		fmt.Printf("[batch] found: %d keys; [cursor] %d; now at: %d\n", len(keys), cursor, n)
		fmt.Printf("found and deleted keys: %v\n\n", keys)
		if cursor == 0 {
			break
		}
	}
	fmt.Printf("found %d keys\n", n)
}
