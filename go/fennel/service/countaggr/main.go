package main

import (
	"log"
	"sync"
	"time"

	"fennel/controller/counter"
	"fennel/instance"
	counterlib "fennel/lib/counter"
	"fennel/test"
)

func aggregate(instance instance.Instance) {
	wg := sync.WaitGroup{}
	types := counter.Types()
	wg.Add(len(types))
	for _, ct := range types {
		go func(ct counterlib.CounterType) {
			defer wg.Done()
			err := counter.Aggregate(instance, ct)
			if err != nil {
				log.Printf("Error found in aggregate for counterlib type: %v. Err: %v", ct, err)
			}
		}(ct)
	}
	wg.Wait()
}

func main() {
	// TODO: don't use default test instance, instead create a real one using env variables etc
	instance, err := test.DefaultInstance()
	if err != nil {
		panic(err)
	}
	for {
		aggregate(instance)
		time.Sleep(time.Minute)
	}
}
