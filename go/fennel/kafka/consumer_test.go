package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"path/filepath"
	"strings"

	segmentkafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/assert"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)
import "github.com/alexflint/go-arg"

var confluentprofile = flag.String("confluentprofile", "", "write cpu profile to file")
var segmentprofile = flag.String("segmentprofile", "", "write cpu profile to file")

type KafkaBenchmarkArgs struct {
	MskKafkaServer   string `arg:"--msk-kafka-server,env:MSK_KAFKA_SERVER_ADDRESS" json:"msk_kafka_server,omitempty"`
	MskKafkaUsername string `arg:"--msk-kafka-user,env:MSK_KAFKA_USERNAME" json:"msk_kafka_username,omitempty"`
	MskKafkaPassword string `arg:"--msk-kafka-password,env:MSK_KAFKA_PASSWORD" json:"msk_kafka_password,omitempty"`
}

func benchmarkConfluent(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	var args KafkaBenchmarkArgs
	_ = arg.Parse(&args)
	c, err := RemoteConsumerConfig{
		BootstrapServer: args.MskKafkaServer,
		Username: args.MskKafkaUsername,
		Password: args.MskKafkaPassword,
		SaslMechanism: SaslScramSha512Mechanism,
		ConsumerConfig: ConsumerConfig{
			Topic: "benchmark-topic2",
			GroupID: "confluent-consumer",
			Scope: resource.NewPlaneScope(ftypes.RealmID(10000001)),
			OffsetPolicy: DefaultOffsetPolicy,
		},
	}.Materialize()
	assert.NoError(b, err)

	data := make([][]byte, 0, 10_000)

	assert.NotEmpty(b, *confluentprofile)
	_ = os.MkdirAll(filepath.Dir(*confluentprofile), os.ModePerm)

	if _, err := os.Stat(*confluentprofile); errors.Is(err, os.ErrNotExist) {
		_ = os.RemoveAll(*confluentprofile)
	}
	f, err := os.Create(*confluentprofile)
	assert.NoError(b, err)
	defer f.Close()

	err = pprof.StartCPUProfile(f)
	assert.NoError(b, err)

	b.StartTimer()

	consumer := c.(RemoteConsumer)
	for true {
		d, err := consumer.ReadBatch(ctx, 1000, 10 * time.Second)
		assert.NoError(b, err)
		data = append(data, d...)

		log, err := consumer.Backlog()
		if err != nil {
			fmt.Printf("warn: backlog failed: %v\n", err)
		} else {
			if log == 0 {
				break
			}
			fmt.Printf("lag: %d\n", log)
		}
		_, err = consumer.Commit()
		if err != nil {
			fmt.Printf("warn: commit failed: %v\n", err)
		}
	}
	_ = consumer.Close()
	b.StopTimer()
	pprof.StopCPUProfile()
}

func benchmarkSegment(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	var args KafkaBenchmarkArgs
	_ = arg.Parse(&args)

	mechanism, err := scram.Mechanism(scram.SHA512, args.MskKafkaUsername, args.MskKafkaPassword)
	assert.NoError(b, err)

	dialer := &segmentkafka.Dialer{
		Timeout: 10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
		SASLMechanism: mechanism,
	}

	//conn, err := dialer.Dial("tcp", strings.Split(args.MskKafkaServer, ",")[0])
	//if err != nil {
	//	panic(err.Error())
	//}
	//defer conn.Close()
	//controller, err := conn.Controller()
	//assert.NoError(b, err)

	r := segmentkafka.NewReader(segmentkafka.ReaderConfig{
		Brokers: strings.Split(args.MskKafkaServer, ","),
		Topic: "benchcmark-topic2",
		GroupID: "testing-segment-cgrp",
		Partition: 0,
		MinBytes: 10e3,  // 10KB
		MaxBytes: 10e6,  // 10MB,
		Dialer: dialer,
		Logger: segmentkafka.LoggerFunc(func(s string, i ...interface{}) {
			fmt.Printf("[logger] msg: %s, %v\n", s, i)
		}),
		ErrorLogger: segmentkafka.LoggerFunc(func(s string, i ...interface{}) {
			fmt.Printf("[errlogger] msg: %s, %v\n", s, i)
		}),
	})

	defer func(r *segmentkafka.Reader) {
		_ = r.Close()
	}(r)

	data := make([][]byte, 0, 10_000)

	assert.NotEmpty(b, *segmentprofile)
	_ = os.MkdirAll(filepath.Dir(*segmentprofile), os.ModePerm)

	if _, err := os.Stat(*segmentprofile); errors.Is(err, os.ErrNotExist) {
		_ = os.RemoveAll(*segmentprofile)
	}
	f, err := os.Create(*segmentprofile)
	assert.NoError(b, err)
	defer f.Close()

	err = pprof.StartCPUProfile(f)
	assert.NoError(b, err)

	b.StartTimer()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("err: %v\n", err)
			break
		}
		lag, err := r.ReadLag(ctx)
		if err != nil {
			fmt.Printf("lag err: %v\n", err)
			break
		}
		fmt.Printf("lag: %v\n", lag)
		data = append(data, m.Value)
	}
	fmt.Printf("data: %d\n", len(data))
	b.StopTimer()
	pprof.StopCPUProfile()
}

func BenchmarkConsumers(b *testing.B) {
	// setup args for remote kafka cluster
	var args KafkaBenchmarkArgs
	_ = arg.Parse(&args)
	scope := resource.NewPlaneScope(ftypes.RealmID(10000001))
	topic := "benchmark-topic2"

	r, err := RemoteProducerConfig{
		Topic: topic,
		BootstrapServer: args.MskKafkaServer,
		Username: args.MskKafkaUsername,
		Password: args.MskKafkaPassword,
		SaslMechanism: SaslScramSha512Mechanism,
		Scope: resource.NewPlaneScope(ftypes.RealmID(10000001)),
	}.Materialize()

	defer func() {
		configmap := &kafka.ConfigMap{
			"bootstrap.servers": args.MskKafkaServer,
			"sasl.username":     args.MskKafkaUsername,
			"sasl.password":     args.MskKafkaPassword,
			"security.protocol": SecurityProtocol,
			"sasl.mechanisms":   SaslScramSha512Mechanism,
		}
		admin, err := kafka.NewAdminClient(configmap)
		if err != nil {
			panic(err)
		}
		_, err = admin.DeleteTopics(context.Background(), []string{scope.PrefixedName(topic)})
		if err != nil {
			panic(err)
		}
	}()

	assert.NoError(b, err)
	ctx := context.Background()
	producer := r.(RemoteProducer)
	data := make([][]byte, 10_000)
	for i := 0; i < 10_000; i++ {
		d := []byte(fmt.Sprintf("i_%s", utils.RandString(3)))
		err := producer.Log(ctx, d, nil)
		assert.NoError(b, err)
		data[i] = d
	}

	// create and write 10000 messages to a topic

	// each benchmark will create a consumer group with a different name and try to read from it
	// b.Run("confluent-go", benchmarkConfluent)
	b.Run("segment-go", benchmarkSegment)
	//b.Run("shopify-go", benchmarkConfluent)
}