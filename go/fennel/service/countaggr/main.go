package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"

	"go.uber.org/zap"

	"fennel/controller/aggregate"
	"fennel/kafka"
	"fennel/lib/action"
	libaggregate "fennel/lib/aggregate"
	"fennel/tier"

	// ensure that all operators are present in the binary
	_ "fennel/opdefs"

	"github.com/alexflint/go-arg"
)

func logKafkaLag(t tier.Tier, consumer kafka.FConsumer) {
	backlog, err := consumer.Backlog()
	if err != nil {
		t.Logger.Error("failed to read kafka backlog", zap.Error(err))
	}
	t.Logger.Info("aggregator_backlog",
		zap.String("consumer_group", consumer.GroupID()),
		zap.Int("backlog", backlog),
	)
}

func main() {
	var flags tier.TierArgs
	// Parse flags / environment variables.
	arg.MustParse(&flags)
	tr, err := tier.CreateFromArgs(&flags)
	if err != nil {
		panic(err)
	}
	// start a server so that promethus collector can ingest metrics
	go func() {
		log.Println(http.ListenAndServe("localhost:2411", nil))
	}()
	// Note: don't delete this log line - e2e tests rely on this to be printed
	// to know that server has initialized and is ready to take traffic
	log.Println("server is ready...")

	aggs, err := aggregate.RetrieveAll(context.Background(), tr)
	if err != nil {
		panic(err)
	}
	for _, agg := range aggs {
		consumer, err := tr.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, string(agg.Name), "earliest")
		if err != nil {
			log.Printf("unable to start consumer for aggregate: %s. Error: %v", agg.Name, err)
			continue
		}
		go func(tr tier.Tier, consumer kafka.FConsumer, agg libaggregate.Aggregate) {
			defer consumer.Close()
			for {
				ctx := context.TODO()
				err := aggregate.Update(ctx, tr, consumer, agg)
				if err != nil {
					log.Printf("Error found in aggregate: %s. Err: %v", agg.Name, err)
				}
				logKafkaLag(tr, consumer)
			}
		}(tr, consumer, agg)
	}
}
