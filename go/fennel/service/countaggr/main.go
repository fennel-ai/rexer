package main

import (
	"log"
	"sync"
	"time"

	"fennel/controller/aggregate"
	libaggregate "fennel/lib/aggregate"
	"fennel/tier"
	"github.com/alexflint/go-arg"
)

func processOnce(tier tier.Tier) {
	wg := sync.WaitGroup{}
	types := libaggregate.ValidTypes
	for _, t := range types {
		aggs, err := aggregate.RetrieveAll(tier, t)
		if err != nil {
			panic(err)
		}
		for _, agg := range aggs {
			wg.Add(1)
			go func(agg libaggregate.Aggregate) {
				defer wg.Done()
				err := aggregate.Update(tier, agg)
				if err != nil {
					log.Printf("Error found in aggregate for agg type: %v and name: %s. Err: %v", agg.Type, agg.Name, err)
				}
			}(agg)
		}
	}
	wg.Wait()
}

func main() {
	var flags struct {
		tier.TierArgs
	}
	// Parse flags / environment variables.
	arg.MustParse(&flags)
	tier, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(err)
	}
	for {
		processOnce(tier)
		time.Sleep(time.Minute)
	}
}
