package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"fennel/controller/aggregate"
	libaggregate "fennel/lib/aggregate"
	"fennel/tier"

	// we need this to ensure that all operators are built with aggregator
	_ "fennel/opdefs"

	"github.com/alexflint/go-arg"
)

func processOnce(tier tier.Tier) {
	ctx := context.TODO()
	wg := sync.WaitGroup{}
	aggs, err := aggregate.RetrieveAll(ctx, tier)
	if err != nil {
		panic(err)
	}
	for _, agg := range aggs {
		wg.Add(1)
		go func(agg libaggregate.Aggregate) {
			defer wg.Done()
			err := aggregate.Update(ctx, tier, agg)
			if err != nil {
				log.Printf("Error found in aggregate for agg type: %v and name: %s. Err: %v", agg.Options.AggType, agg.Name, err)
			}
		}(agg)
	}
	wg.Wait()
}

// TODO(nikhil): enable this back
func monitorKafkaLag(t tier.Tier) {
	ticker := time.NewTicker(30 * time.Second)
	//logger := t.Logger
	for {
		<-ticker.C
		//for topic, consumer := range t.Consumers {
		//	backlog, err := consumer.Backlog()
		//	if err != nil {
		//		logger.Error("failed to read kafka backlog", zap.Error(err))
		//	}
		//	logger.Info("kafka backlog",
		//		zap.String("topic", topic),
		//		zap.Int("backlog", backlog),
		//	)
		//}
	}
}

func main() {
	var flags tier.TierArgs
	// Parse flags / environment variables.
	arg.MustParse(&flags)
	tier, err := tier.CreateFromArgs(&flags)
	if err != nil {
		panic(err)
	}
	// Start monitoring kafka lag in a go-routine.
	go monitorKafkaLag(tier)
	go func() {
		log.Println(http.ListenAndServe("localhost:2411", nil))
	}()
	// Note: don't delete this log line - e2e tests rely on this to be printed
	// to know that server has initialized and is ready to take traffic
	log.Println("server is ready...")
	for {
		processOnce(tier)
		time.Sleep(10 * time.Second)
	}
}
