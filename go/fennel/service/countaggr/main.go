package main

import (
	"context"
	"fmt"
	"log"
	_ "net/http/pprof"
	"time"

	action2 "fennel/controller/action"
	"fennel/controller/aggregate"
	profile2 "fennel/controller/profile"
	"fennel/kafka"
	"fennel/lib/action"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/timer"
	_ "fennel/opdefs" // ensure that all operators are present in the binary
	"fennel/service/common"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var backlog_stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "aggregator_backlog",
	Help: "Stats about kafka consumer group backlog",
}, []string{"consumer_group"})

func logKafkaLag(t tier.Tier, consumer kafka.FConsumer) {
	backlog, err := consumer.Backlog()
	if err != nil {
		t.Logger.Error("failed to read kafka backlog", zap.Error(err))
	}
	backlog_stats.WithLabelValues(consumer.GroupID()).Set(float64(backlog))
}

func processAggregate(tr tier.Tier, agg libaggregate.Aggregate) error {
	consumer, err := tr.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, string(agg.Name), kafka.DefaultOffsetPolicy)
	if err != nil {
		return fmt.Errorf("unable to start consumer for aggregate: %s. Error: %v", agg.Name, err)
	}
	go func(tr tier.Tier, consumer kafka.FConsumer, agg libaggregate.Aggregate) {
		defer consumer.Close()
		run := 0
		for {
			tr.Logger.Info("Processing aggregate", zap.String("aggregate_name", string(agg.Name)), zap.Int("run", run))
			ctx := context.TODO()
			err := aggregate.Update(ctx, tr, consumer, agg)
			if err != nil {
				log.Printf("Error found in aggregate: %s. Err: %v", agg.Name, err)
			}
			logKafkaLag(tr, consumer)
			tr.Logger.Info("Processed aggregate", zap.String("aggregate_name", string(agg.Name)), zap.Int("run", run))
			run += 1
		}
	}(tr, consumer, agg)
	return nil
}

func startActionDBInsertion(tr tier.Tier) error {
	consumer, err := tr.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, "_put_actions_in_db", kafka.DefaultOffsetPolicy)
	if err != nil {
		return fmt.Errorf("unable to start consumer for inserting actions in DB: %v", err)
	}
	go func(tr tier.Tier, consumer kafka.FConsumer) {
		defer consumer.Close()
		ctx := context.Background()
		for {
			t := timer.Start(ctx, tr.ID, "countaggr.TransferToDB")
			if err := action2.TransferToDB(ctx, tr, consumer); err != nil {
				tr.Logger.Error("error while reading/writing actions to insert in db:", zap.Error(err))
			}
			t.Stop()
		}
	}(tr, consumer)
	return nil
}

func startProfileDBInsertion(tr tier.Tier) error {
	consumer, err := tr.NewKafkaConsumer(profile.PROFILELOG_KAFKA_TOPIC, "_put_profiles_in_db", kafka.DefaultOffsetPolicy)
	if err != nil {
		return fmt.Errorf("unable to start consumer for inserting profiles in DB: %v", err)
	}
	go func(tr tier.Tier, consumer kafka.FConsumer) {
		defer consumer.Close()
		ctx := context.Background()
		for {
			t := timer.Start(ctx, tr.ID, "countaggr.TransferProfilesToDB")
			if err := profile2.TransferToDB(ctx, tr, consumer); err != nil {
				tr.Logger.Error("error while reading/writing actions to insert in db:", zap.Error(err))
			}
			t.Stop()
		}
	}(tr, consumer)
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

	// first kick off a goroutine to transfer actions from kafka to DB
	if err = startActionDBInsertion(tr); err != nil {
		panic(err)
	}

	if err = startProfileDBInsertion(tr); err != nil {
		panic(err)
	}

	// Set of aggregates that are currently being processed by the system.
	processedAggregates := make(map[ftypes.AggName]struct{})
	ticker := time.NewTicker(time.Second * 15)
	for {
		aggs, err := aggregate.RetrieveAll(context.Background(), tr)
		if err != nil {
			panic(err)
		}
		for _, agg := range aggs {
			if _, ok := processedAggregates[agg.Name]; !ok {
				log.Printf("Retrieved a new aggregate: %v", aggs)
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
