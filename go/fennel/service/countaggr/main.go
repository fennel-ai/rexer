package main

import (
	"context"
	"fennel/airbyte"
	"fennel/lib/value"
	"fmt"
	"log"
	"math/rand"
	_ "net/http/pprof"
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

	connector "fennel/controller/data_integration"
	"fennel/lib/data_integration"
	connectorModel "fennel/model/data_integration"

	_ "fennel/opdefs" // ensure that all operators are present in the binary
	"fennel/resource"
	"fennel/service/common"
	"fennel/tier"

	"github.com/Unleash/unleash-client-go/v3"
	"github.com/alexflint/go-arg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var backlog_stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "aggregator_backlog",
	Help: "Stats about kafka consumer group backlog",
}, []string{"consumer_group"})

var aggregates_disabled = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "aggregates_disabled",
	Help: "Whether aggregates are disabled",
})

func logKafkaLag(t tier.Tier, consumer kafka.FConsumer) {
	backlog, err := consumer.Backlog()
	if err != nil {
		t.Logger.Error("Failed to read kafka backlog", zap.String("Name", consumer.GroupID()), zap.Error(err))
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

func processAggregate(tr tier.Tier, agg libaggregate.Aggregate, stopCh <-chan struct{}) error {
	var consumer kafka.FConsumer
	var err error

	if agg.IsProfileBased() {
		consumer, err = tr.NewKafkaConsumer(kafka.ConsumerConfig{
			Scope:        resource.NewTierScope(tr.ID),
			Topic:        profile.PROFILELOG_KAFKA_TOPIC,
			GroupID:      string(agg.Name),
			OffsetPolicy: kafka.DefaultOffsetPolicy,
		})
	} else {
		// We assume that aggregates are by default action based.
		consumer, err = tr.NewKafkaConsumer(kafka.ConsumerConfig{
			Scope:        resource.NewTierScope(tr.ID),
			Topic:        action.ACTIONLOG_KAFKA_TOPIC,
			GroupID:      string(agg.Name),
			OffsetPolicy: kafka.DefaultOffsetPolicy,
		})
	}

	if err != nil {
		return fmt.Errorf("unable to start consumer for aggregate: %s. Error: %v", agg.Name, err)
	}
	go func(tr tier.Tier, consumer kafka.FConsumer, agg libaggregate.Aggregate, stopCh <-chan struct{}) {
		defer consumer.Close()
		// Ticker to log kafka lag every 1 minute.
		kt := time.NewTicker(1 * time.Minute)
		defer kt.Stop()
		commitFailures := 0
		for run := 0; true; {
			select {
			case <-stopCh:
				return
			case <-kt.C:
				logKafkaLag(tr, consumer)
			default:
				run++
				ctx := context.Background()
				if agg.IsProfileBased() {
					// The number of actions and profiles need to be tuned.
					// They should not be too many such that operations like op.model.predict cant handle them.
					profiles, err := profile2.ReadBatch(ctx, consumer, 300, time.Second*10)
					if err != nil {
						tr.Logger.Error("Error reading profiles", zap.Error(err))
						continue
					}
					if len(profiles) == 0 {
						continue
					}
					tr.Logger.Debug("Processing aggregate", zap.String("name", string(agg.Name)), zap.Int("run", run), zap.Int("profiles", len(profiles)))

					err = aggregate.Update(ctx, tr, profiles, agg)
					if err != nil {
						aggregate_errors.WithLabelValues(string(agg.Name)).Inc()
						tr.Logger.Warn("Error found in profile aggregate", zap.String("name", string(agg.Name)), zap.Error(err))
						continue
					}
				} else {
					actions, err := action2.ReadBatch(ctx, consumer, 10000, time.Second*10)
					if err != nil {
						tr.Logger.Error("Error while reading batch of actions:", zap.Error(err))
						continue
					}
					if len(actions) == 0 {
						continue
					}
					tr.Logger.Debug("Processing aggregate", zap.String("name", string(agg.Name)), zap.Int("run", run), zap.Int("actions", len(actions)))

					err = aggregate.Update(ctx, tr, actions, agg)
					if err != nil {
						aggregate_errors.WithLabelValues(string(agg.Name)).Inc()
						tr.Logger.Warn("Error found in action aggregate", zap.String("name", string(agg.Name)), zap.Error(err))
						continue
					}
				}
				_, err = consumer.Commit()
				if err != nil {
					commitFailures++
					if commitFailures > 10 {
						// We panic in case we fail to commit the offset, since
						// a subsequent crash-recovery could cause us to double-
						// process a lot of data. The right solution to this would
						// be to start managing the offsets ourselves instead of
						// relying on the broker.
						tr.Logger.Panic("Failed to commit kafka offset", zap.Error(err))
					}
				}
				tr.Logger.Debug("Processed aggregate", zap.String("name", string(agg.Name)), zap.Int("run", run))
			}
		}
	}(tr, consumer, agg, stopCh)
	return nil
}

var totalDedupedStreamLogs = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "deduped_stream_total",
		Help: "Total number of deduped stream logs.",
	},
	[]string{"path", "action_type"},
)

func processConnector(tr tier.Tier, conn data_integration.Connector, stopCh <-chan struct{}) error {
	consumer, err := tr.NewKafkaConsumer(kafka.ConsumerConfig{
		Scope:        resource.NewTierScope(tr.ID),
		Topic:        airbyte.AIRBYTE_KAFKA_TOPIC,
		GroupID:      conn.Name,
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	if err != nil {
		return fmt.Errorf("unable to start consumer for connector: %s. Error: %v", conn.Name, err)
	}

	go func(tr tier.Tier, consumer kafka.FConsumer, conn data_integration.Connector, stopCh <-chan struct{}) {
		defer consumer.Close()
		// Ticker to log kafka lag every 30 seconds.
		kt := time.NewTicker(30 * time.Second)
		defer kt.Stop()
		ctx := context.Background()
		commitFailures := 0
		for run := 0; true; {
			select {
			case <-stopCh:
				return
			case <-kt.C:
				logKafkaLag(tr, consumer)
			default:
				run++
				values, hashes, err := connector.ReadBatch(ctx, consumer, conn.StreamName, conn.Name, 10000, time.Second*20)
				if err != nil {
					tr.Logger.Error("Error while reading batch of actions:", zap.Error(err))
					continue
				}

				if len(values) == 0 {
					continue
				}
				tr.Logger.Debug("Processing connector", zap.String("name", conn.Name), zap.Int("run", run), zap.Int("values", len(values)))

				var keys []string
				var vals []interface{}
				var ttls []time.Duration
				var ids []int
				for i, h := range hashes {
					keys = append(keys, string(h[:]))
					vals = append(vals, 1)
					ttls = append(ttls, airbyte.AIRBYTE_DEDUP_TTL)
					ids = append(ids, i)
				}
				// Check for dedup with a pipeline
				ok, err := tr.Redis.SetNXPipelined(ctx, keys, vals, ttls)
				if err != nil {
					tr.Logger.Error("Error while checking for dedup from stream", zap.Error(err))
					return
				}

				var batch []value.Value
				for i := range ok {
					if ok[i] {
						// If dedup key of an action was not set, add to batch
						batch = append(batch, values[ids[i]])
					} else {
						totalDedupedStreamLogs.WithLabelValues("airbyte_log", conn.Name).Inc()
					}
				}

				// Process the deduped stream
				table, err := aggregate.Transform(tr, batch, conn.Query)
				if err != nil {
					tr.Logger.Error("Error while transforming stream", zap.Error(err))
					continue
				}
				tr.Logger.Debug("Processing connector after transform", zap.String("name", conn.Name), zap.Int("run", run), zap.Int("transformed values", table.Len()))

				// fwd to controller
				switch conn.Destination {
				case airbyte.ACTION_DESTINATION:
					// Convert to actions
					actionBatch := make([]action.Action, table.Len())
					for i := 0; i < table.Len(); i++ {
						row, _ := table.At(i)
						actionBatch[i], err = action.FromValueDict(row.(value.Dict))
						if err != nil {
							tr.Logger.Error("Error while converting to action:", zap.Error(err))
							continue
						}
					}
					if err = action2.BatchInsert(ctx, tr, actionBatch); err != nil {
						tr.Logger.Error("Error while inserting actions:", zap.Error(err))
						continue
					}
				case airbyte.PROFILE_DESTINATION:
					// Convert to profiles
					profileBatch := make([]profile.ProfileItem, table.Len())
					for i := 0; i < table.Len(); i++ {
						row, _ := table.At(i)
						profileBatch[i], err = profile.FromValueDict(row.(value.Dict))
						if err != nil {
							tr.Logger.Error("Error while converting to profile:", zap.Error(err))
							continue
						}
					}
					if err = profile2.SetMulti(ctx, tr, profileBatch); err != nil {
						tr.Logger.Error("Error while inserting profile", zap.Error(err))
					}
				}
				_, err = consumer.Commit()
				if err != nil {
					commitFailures++
					if commitFailures > 10 {
						tr.Logger.Panic("Failed to commit kafka offset for airbyte streams", zap.Error(err))
					}
				}
			}
		}
	}(tr, consumer, conn, stopCh)
	return nil
}

func startActionDBInsertion(tr tier.Tier) error {
	consumer, err := tr.NewKafkaConsumer(kafka.ConsumerConfig{
		Scope:        resource.NewTierScope(tr.ID),
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
		Scope:        resource.NewTierScope(tr.ID),
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

func startAggregateProcessing(tr tier.Tier) error {
	// Map from aggregate name to channel to stop the aggregate processing.
	processedAggregates := make(map[ftypes.AggName]chan<- struct{})
	ticker := time.NewTicker(time.Second * 15)
	for ; true; <-ticker.C {
		aggs, err := aggregate.RetrieveActive(context.Background(), tr)
		if err != nil {
			return err
		}
		if unleash.IsEnabled("disable-aggregates") {
			aggregates_disabled.Set(float64(1))
			continue
		}
		aggNames := make(map[ftypes.AggName]struct{}, len(aggs))
		for _, agg := range aggs {
			aggNames[agg.Name] = struct{}{}
			if _, ok := processedAggregates[agg.Name]; !ok {
				log.Printf("Retrieved a new aggregate: %s", agg.Name)
				ch := make(chan struct{})
				err := processAggregate(tr, agg, ch)
				if err != nil {
					tr.Logger.Error("Could not start aggregate processing", zap.String("aggregateName", string(agg.Name)), zap.Error(err))
				}
				processedAggregates[agg.Name] = ch
			}
		}
		// Stop processing any aggregates that are no longer active.
		for a := range processedAggregates {
			if _, ok := aggNames[a]; !ok {
				close(processedAggregates[a])
				delete(processedAggregates, a)
			}
		}
	}
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

func startConnectorProcessing(tr tier.Tier) error {
	go func(tr tier.Tier) {
		// Map from connector name to channel to stop the connector processing.
		processedConnectors := make(map[string]chan<- struct{})
		ticker := time.NewTicker(time.Second * 30)
		for ; true; <-ticker.C {
			conns, err := connectorModel.RetrieveActive(context.Background(), tr)
			if err != nil {
				tr.Logger.Error("Could not retrieve connectors", zap.Error(err))
				continue
			}
			connNames := make(map[string]struct{}, len(conns))
			for _, conn := range conns {
				connNames[conn.Name] = struct{}{}
				if _, ok := processedConnectors[conn.Name]; !ok {
					log.Printf("Retrieved a new connector: %s", conn.Name)
					ch := make(chan struct{})
					err := processConnector(tr, conn, ch)
					if err != nil {
						tr.Logger.Error("Could not start connector processing", zap.String("Connector Name", string(conn.Name)), zap.Error(err))
					}
					processedConnectors[conn.Name] = ch
				}
			}
			// Stop processing any connector that are no longer active.
			for a := range processedConnectors {
				if _, ok := connNames[a]; !ok {
					close(processedConnectors[a])
					delete(processedConnectors, a)
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
		common.HealthCheckArgs
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
	// Start health checker to export readiness and liveness state for the container running the server
	common.StartHealthCheckServer(8083)
	// Start a pprof server to export the standard pprof endpoints.
	profiler := common.CreateProfiler(flags.PprofArgs)
	profiler.StartPprofServer()

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

	if err = startConnectorProcessing(tr); err != nil {
		panic(err)
	}

	if err = startAggregateProcessing(tr); err != nil {
		panic(err)
	}
}
