package main

import (
	"fennel/controller/aggregate"
	libaggregate "fennel/lib/aggregate"
	"log"
	"sync"
	"time"

	"fennel/plane"
	"fennel/test"
)

func processOnce(instance plane.Plane) {
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
	// TODO: don't use default test instance, instead create a real one using env variables etc
	instance, err := test.MockPlane()
	if err != nil {
		panic(err)
	}
	for {
		processOnce(instance)
		time.Sleep(time.Minute)
	}
}
