package main

import (
	"log"
	"sync"
	"time"

	"fennel/controller/aggregate"
	libaggregate "fennel/lib/aggregate"
	"fennel/tier"
	arg "github.com/alexflint/go-arg"
)

func processOnce(instance tier.Tier) {
	wg := sync.WaitGroup{}
	types := libaggregate.ValidTypes
	for _, t := range types {
		aggs, err := aggregate.RetrieveAll(instance, t)
		if err != nil {
			panic(err)
		}
		for _, agg := range aggs {
			wg.Add(1)
			go func(agg libaggregate.Aggregate) {
				defer wg.Done()
				err := aggregate.Update(instance, agg)
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
		tier.PlaneArgs
	}
	// Parse flags / environment variables.
	arg.MustParse(&flags)
	plane, err := tier.CreateFromArgs(&flags.PlaneArgs)
	if err != nil {
		panic(err)
	}
	for {
		processOnce(plane)
		time.Sleep(time.Minute)
	}
}
