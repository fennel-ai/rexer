package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"strconv"
	"strings"
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
	"fennel/lib/value"
	_ "fennel/opdefs" // ensure that all operators are present in the binary
	"fennel/service/common"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"go.uber.org/zap"
)

var SUCCESS_PREFIX = "_SUCCESS-"
var BATCH_SIZE = 1000

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

// TODO(Mohit): Deprecate this in-favor of using a log management solution, where alerts will be created on error event
// and will have better visibility into the error
var aggregate_errors = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "aggregate_errors",
		Help: "Stats on aggregate failures",
	}, []string{"aggregate"})

func processAggregate(tr tier.Tier, agg libaggregate.Aggregate) error {
	consumer, err := tr.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        action.ACTIONLOG_KAFKA_TOPIC,
		GroupID:      string(agg.Name),
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
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
				aggregate_errors.WithLabelValues(string(agg.Name)).Add(1)
				log.Printf("Error found in aggregate: %s. Err: %v", agg.Name, err)
			}
			logKafkaLag(tr, consumer)
			tr.Logger.Info("Processed aggregate", zap.String("aggregate_name", string(agg.Name)), zap.Int("run", run))
			run += 1
		}
	}(tr, consumer, agg)
	return nil
}

// func getLatestVersion(files []string) (int, error) {

// }

type ItemScore struct {
	ItemName *string  `parquet:"name=topk_keys, type=BYTE_ARRAY, convertedtype=UTF8"`
	Score    *float64 `parquet:"name=topk_score, type=FLOAT"`
}

type Example struct {
	Key      *string     `parquet:"name=groupkey, type=BYTE_ARRAY"`
	ItemName []ItemScore `parquet:"name=topk, type=LIST"`
}

func pollOfflineAggregateOutputs(tr tier.Tier, agg libaggregate.Aggregate, duration uint64) error {
	go func(tr tier.Tier, agg libaggregate.Aggregate, duration uint64) {
		// var lastKnowVersion string
		ticker := time.NewTicker(time.Minute * 1)
		for {
			<-ticker.C
			aggRetrieved, err := aggregate.Retrieve(context.Background(), tr, agg.Name)
			if err != nil && err != libaggregate.ErrNotFound {
				break
			}
			currUpdateVersion := aggRetrieved.ServingData.UpdateVersion[duration]
			_, _, day := time.Now().Date()
			//prefix := "p-2-offline-aggregate-output/t_107/similar_movies-604800/"
			prefix := fmt.Sprintf("%s/t_%d/%s-%d/day=%d", tr.Args.OfflineAggBucket, int(tr.ID), agg.Name, duration, day)

			// Check for any new updates to the aggregate ------------------

			files, err := tr.S3Client.ListFiles(tr.Args.OfflineAggBucket, prefix)
			fmt.Println("Printing args :: ", tr.Args.OfflineAggBucket, prefix)
			if err != nil {
				tr.Logger.Error("error while listing files in s3 bucket:", zap.Error(err))
				return
			}

			var prefixToUpdate string

			for _, file := range files {
				pathArray := strings.Split(file, "/")
				if len(pathArray) > 0 && strings.HasPrefix(pathArray[len(pathArray)-1], SUCCESS_PREFIX) {
					updateVersion := strings.Replace(pathArray[len(pathArray)-1], SUCCESS_PREFIX, "", 1)
					fmt.Println(file, "::", updateVersion)
					UpdateVersionInt, err := strconv.ParseUint(updateVersion, 10, 64)
					if err != nil {
						tr.Logger.Error("error while converting update version to int:", zap.Error(err))
						return
					}
					if UpdateVersionInt > currUpdateVersion {
						prefixToUpdate = strings.Join(pathArray[:len(pathArray)-1], "/")
						currUpdateVersion = UpdateVersionInt
					}
				}
			}

			// Couldn't find any new updates, so return
			if prefixToUpdate == "" {
				fmt.Println("No new updates found for ")
				continue
			}

			// Download the files to disk ------------------
			var filesToDownload []string
			var fileNames []string

			for _, file := range files {
				if strings.HasPrefix(file, prefixToUpdate) && !strings.HasSuffix(file, fmt.Sprintf("%s%s", SUCCESS_PREFIX, updateVersion)) {
					filesToDownload = append(filesToDownload, file)
					fileNames = append(fileNames, strings.Replace(file, prefixToUpdate, "", 1))
				}
			}

			folder := "/tmp"
			err = tr.S3Client.BatchDiskDownload(filesToDownload, tr.Args.OfflineAggBucket, folder)
			if err != nil {
				fmt.Println(err)
			}

			// Read the files from disk and prepare the data ------------------

			redisWriteSuccess := true
			for _, file := range fileNames {
				fr, err := local.NewLocalFileReader(folder + "/" + file)

				pr, err := reader.NewParquetReader(fr, new(Example), 4)
				if err != nil {
					log.Fatal(err)
				}
				numRows := int(pr.GetNumRows())

				fmt.Println("Number of rows", numRows)

				// Create temp file to write to Redis
				f, err := os.Create("/tmp/" + file + "-redis.txt")
				if err != nil {
					log.Fatal(err)
				}

				for i := 0; i < numRows; i++ {
					examples := make([]Example, BATCH_SIZE)
					if i+BATCH_SIZE < numRows {
						i += BATCH_SIZE
					} else {
						i = numRows
					}

					if err = pr.Read(&examples); err != nil {
						log.Println("Read error ::", err)
					}

					for _, example := range examples {
						v := value.NewList()
						for _, item := range example.ItemName {
							if item.ItemName != nil {
								v.Append(value.NewDict(map[string]value.Value{
									"item":  value.String(*item.ItemName),
									"score": value.Double(*item.Score),
								}))
							}
						}
						encodedString := base64.StdEncoding.EncodeToString(value.ToJSON(v))
						f.WriteString("SET " + string(*example.Key) + " " + encodedString + "\n")
					}
				}
				f.Close()
				pr.ReadStop()
				fr.Close()
				cmd := "cat /tmp/" + file + "-redis.txt" + " | redis-cli --pipe"
				out, err := exec.Command("bash", "-c", cmd).Output()
				fmt.Println(string(out))

				if !strings.Contains(string(out), "errors: 0, replies: "+fmt.Sprintf("%d", numRows)) {
					// Write to Redis failed
					redisWriteSuccess = false
					break
				}
			}

			if redisWriteSuccess {
				// Update DB with the new version
				//err = aggregate.UpdateAggregateVersion(agg.Name, updateVersion)
				fmt.Println("Update aggregate version")
			}

		}
	}(tr, agg, duration)
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

func startAggregateProcessing(tr tier.Tier) error {
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

				if agg.Options.CronSchedule != "" {
					log.Printf("Retrieved a new offline aggregate: %v", aggs)
					for _, duration := range agg.Options.Durations {
						pollOfflineAggregateOutputs(tr, agg, duration)
					}
				}
				processedAggregates[agg.Name] = struct{}{}
			}
		}
		<-ticker.C
	}
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

	if err = startAggregateProcessing(tr); err != nil {
		panic(err)
	}
}
