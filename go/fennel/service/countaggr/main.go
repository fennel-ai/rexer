package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	_ "net/http/pprof"
	"sync"
	"time"

	action2 "fennel/controller/action"
	"fennel/controller/aggregate"
	profile2 "fennel/controller/profile"
	"fennel/kafka"
	"fennel/lib/action"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/phaser"
	"fennel/lib/profile"
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
		t.Logger.Error("Failed to read kafka backlog", zap.Error(err))
	}
	backlog_stats.WithLabelValues(consumer.GroupID()).Set(float64(backlog))
}

// TODO(Mohit): Deprecate this in-favor of using a log management solution, where alerts will be created on error event
// and will have better visibility into the error
var aggregate_errors = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "aggregate_errors",
		Help: "Stats on aggregate failures",
	}, []string{"aggregate"})

func process[I action.Action | profile.ProfileItem](tr tier.Tier, aggregates []libaggregate.Aggregate, items []I) {
	wg := sync.WaitGroup{}
	wg.Add(len(aggregates))
	ctx := context.Background()
	for i := 0; i < len(aggregates); i++ {
		go func(agg libaggregate.Aggregate) {
			defer wg.Done()
			tr.Logger.Debug("Processing aggregate", zap.String("name", string(agg.Name)))
			err := aggregate.Update(ctx, tr, items, agg)
			if err != nil {
				aggregate_errors.WithLabelValues(string(agg.Name)).Add(1)
				tr.Logger.Warn("Error found in aggregate", zap.String("name", string(agg.Name)), zap.Error(err))
			}
			tr.Logger.Debug("Processed aggregate", zap.String("name", string(agg.Name)))
		}(aggregates[i])
	}
	wg.Wait()
}

func getAggregatesOfType(tr tier.Tier, src ftypes.Source) ([]libaggregate.Aggregate, error) {
	aggregates, err := aggregate.RetrieveActive(context.Background(), tr)
	if err != nil {
		return nil, err
	}
	filtered := aggregates[:0]
	for i := 0; i < len(aggregates); i++ {
		if aggregates[i].Source == src {
			filtered = append(filtered, aggregates[i])
		}
	}
	return filtered, nil
}

func processProfileAggregates(tr tier.Tier) error {
	aggregates, err := getAggregatesOfType(tr, libaggregate.SOURCE_PROFILE)
	if err != nil {
		return fmt.Errorf("unable to retrieve aggregates for profiles: %w", err)
	}
	consumer, err := tr.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        profile.PROFILELOG_KAFKA_TOPIC,
		GroupID:      fmt.Sprintf("%v-profile-agg-consumer", tr.ID),
		OffsetPolicy: kafka.LatestOffsetPolicy,
	})
	if err != nil {
		return fmt.Errorf("unable to start consumer for profiles: %w", err)
	}
	go func(tr tier.Tier, consumer kafka.FConsumer) {
		defer consumer.Close()
		// Ticker to log kafka lag every 1 minute.
		kt := time.NewTicker(1 * time.Minute)
		defer kt.Stop()
		ctx := context.Background()
		for {
			select {
			case <-kt.C:
				aggregates, err = getAggregatesOfType(tr, libaggregate.SOURCE_PROFILE)
				if err != nil {
					// Log error but continue processing previously fetched aggregates.
					tr.Logger.Error("Unable to retrieve aggregates for profiles:", zap.Error(err))
				}
				logKafkaLag(tr, consumer)
			default:
				// The number of profiles need to be tuned. They should not be too
				// many because otherwise operations like op.model.predict can't
				// handle them.
				profiles, err := profile2.ReadBatch(ctx, consumer, 500, time.Second*10)
				if err != nil {
					tr.Logger.Error("Error reading profiles", zap.Error(err))
				}
				if len(profiles) == 0 {
					continue
				}
				process(tr, aggregates, profiles)
				_, err = consumer.Commit()
				if err != nil {
					tr.Logger.Warn("Unable to commit consumer offset:", zap.Error(err))
				}
			}
		}
	}(tr, consumer)
	return nil
}

func processActionAggregates(tr tier.Tier) error {
	aggregates, err := getAggregatesOfType(tr, libaggregate.SOURCE_ACTION)
	if err != nil {
		return fmt.Errorf("unable to retrieve aggregates for actions: %w", err)
	}
	consumer, err := tr.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        action.ACTIONLOG_KAFKA_TOPIC,
		GroupID:      fmt.Sprintf("%v-action-agg-consumer", tr.ID),
		OffsetPolicy: kafka.LatestOffsetPolicy,
	})
	if err != nil {
		return fmt.Errorf("unable to start consumer for actions: %w", err)
	}
	go func(tr tier.Tier, consumer kafka.FConsumer) {
		defer consumer.Close()
		// Ticker to wake up every 1 minute to log kafka lag and refresh aggregates.
		kt := time.NewTicker(1 * time.Minute)
		defer kt.Stop()
		ctx := context.Background()
		for {
			select {
			case <-kt.C:
				logKafkaLag(tr, consumer)
				aggregates, err = getAggregatesOfType(tr, libaggregate.SOURCE_ACTION)
				if err != nil {
					// Log error but continue processing previously fetched aggregates.
					tr.Logger.Error("Unable to retrieve aggregates for actions:", zap.Error(err))
				}
			default:
				actions, err := action2.ReadBatch(ctx, consumer, 10000, time.Second*10)
				if err != nil {
					tr.Logger.Error("Error reading actions", zap.Error(err))
				}
				if len(actions) == 0 {
					continue
				}
				process(tr, aggregates, actions)
				_, err = consumer.Commit()
				if err != nil {
					tr.Logger.Warn("Unable to commit consumer offset:", zap.Error(err))
				}
			}
		}
	}(tr, consumer)
	return nil
}

func startActionDBInsertion(tr tier.Tier) error {
	consumer, err := tr.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        action.ACTIONLOG_KAFKA_TOPIC,
		GroupID:      "_put_actions_in_db",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	if err != nil {
		return fmt.Errorf("unable to start consumer for inserting actions in DB: %v", err)
	}
	go func(tr tier.Tier, consumer kafka.FConsumer) {
		defer consumer.Close()
		ctx := context.Background()
		// TODO(mohit): The tracing here should be at the span level, which is currently not implemented at a
		// library level
		// The metric exported from here is not an important - hasn't given us much of a signal yet
		for {
			if err := action2.TransferToDB(ctx, tr, consumer); err != nil {
				tr.Logger.Error("error while reading/writing actions to insert in db:", zap.Error(err))
			}
		}
	}(tr, consumer)
	return nil
}

func startProfileDBInsertion(tr tier.Tier) error {
	consumer, err := tr.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        profile.PROFILELOG_KAFKA_TOPIC,
		GroupID:      "_put_profiles_in_db",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	if err != nil {
		return fmt.Errorf("unable to start consumer for inserting profiles in DB: %v", err)
	}
	go func(tr tier.Tier, consumer kafka.FConsumer) {
		defer consumer.Close()
		ctx := context.Background()
		// TODO(mohit): The tracing here should be at the span level, which is currently not implemented at a
		// library level
		// The metric exported from here is not an important - hasn't given us much of a signal yet
		for {
			if err := profile2.TransferToDB(ctx, tr, consumer); err != nil {
				tr.Logger.Error("error while reading/writing actions to insert in db:", zap.Error(err))
			}
		}
	}(tr, consumer)
	return nil
}

func startPhaserProcessing(tr tier.Tier) error {
	go func(tr tier.Tier) {
		processedPhasers := make(map[string]struct{})
		ticker := time.NewTicker(time.Second * 60)
		for ; true; <-ticker.C {
			phasers, err := phaser.RetrieveAll(context.Background(), tr)
			if err != nil {
				tr.Logger.Error("Could not retrieve phasers", zap.Error(err))
				continue
			}
			for _, p := range phasers {
				if _, ok := processedPhasers[p.GetId()]; !ok {
					log.Printf("Retrieved a new phaser: %v", p.GetId())
					phaser.ServeData(tr, p)
					processedPhasers[p.GetId()] = struct{}{}
				}
			}
		}
	}(tr)
	return nil
}

func main() {
	// seed random number generator so that all uses of rand work well
	rand.Seed(time.Now().UnixNano())
	// Parse flags / environment variables.
	var flags struct {
		tier.TierArgs
		common.PrometheusArgs
		common.PprofArgs
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
	// Start a pprof server to export the standard pprof endpoints.
	common.StartPprofServer(flags.PprofPort)

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

	if err = startPhaserProcessing(tr); err != nil {
		panic(err)
	}

	if err = processProfileAggregates(tr); err != nil {
		panic(err)
	}

	if err = processActionAggregates(tr); err != nil {
		panic(err)
	}

}
