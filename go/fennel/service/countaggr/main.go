package main

import (
	"context"
	"fmt"
	"log"
	_ "net/http/pprof"
	"time"

	"fennel/controller/aggregate"
	"fennel/kafka"
	"fennel/lib/action"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	_ "fennel/opdefs" // ensure that all operators are present in the binary
	"fennel/service/common"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"go.uber.org/zap"
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

// Set of aggregates that are currently being processed by the system.
var processedAggregates map[ftypes.AggName]struct{}

func processAggregate(tr tier.Tier, agg libaggregate.Aggregate) error {
	consumer, err := tr.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, string(agg.Name), "earliest")
	if err != nil {
		return fmt.Errorf("unable to start consumer for aggregate: %s. Error: %v", agg.Name, err)
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
	return nil
}

func main() {
	// Parse flags / environment variables.
	var flags struct {
		tier.TierArgs
		common.PrometheusArgs
	}
	// Parse flags / environment variables.
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	tr, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(err)
	}
	// Start a prometheus server.
	common.StartPromMetricsServer(flags.MetricsPort)
	// Note: don't delete this log line - e2e tests rely on this to be printed
	// to know that server has initialized and is ready to take traffic
	log.Println("server is ready...")

	ticker := time.NewTicker(time.Minute)
	for {
		aggs, err := aggregate.RetrieveAll(context.Background(), tr)
		if err != nil {
			panic(err)
		}
		log.Printf("Retrieved aggregates: %v", aggs)
		for _, agg := range aggs {
			if _, ok := processedAggregates[agg.Name]; !ok {
				err := processAggregate(tr, agg)
				if err != nil {
					tr.Logger.Error("Could not start aggregate processing", zap.String("aggregateName", string(agg.Name)), zap.Error(err))
				}
				processedAggregates[agg.Name] = struct{}{}
			}
		}
		<-ticker.C
	}
}
