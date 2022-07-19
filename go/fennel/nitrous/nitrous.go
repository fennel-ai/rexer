package nitrous

import (
	"fmt"
	"log"

	"fennel/hangar"
	"fennel/hangar/cache"
	"fennel/hangar/db"
	"fennel/hangar/encoders"
	"fennel/hangar/layered"
	libkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fennel/s3"

	"github.com/raulk/clock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type NitrousArgs struct {
	s3.S3Args `json:"s3_._s3_args"`

	PlaneID            ftypes.RealmID `arg:"--plane-id,env:PLANE_ID" json:"plane_id,omitempty"`
	KafkaServer        string         `arg:"--kafka-server,env:KAFKA_SERVER_ADDRESS" json:"kafka_server,omitempty"`
	KafkaUsername      string         `arg:"--kafka-user,env:KAFKA_USERNAME" json:"kafka_username,omitempty"`
	KafkaPassword      string         `arg:"--kafka-password,env:KAFKA_PASSWORD" json:"kafka_password,omitempty"`
	BadgerDir          string         `arg:"--badger_dir,env:BADGER_DIR" json:"badger_dir,omitempty"`
	BadgerBlockCacheMB int64          `arg:"--badger_block_cache_mb,env:BADGER_BLOCK_CACHE_MB" json:"badger_block_cache_mb,omitempty"`
	RistrettoMaxCost   uint64         `arg:"--ristretto_max_cost,env:RISTRETTO_MAX_COST" json:"ristretto_max_cost,omitempty"`
	RistrettoAvgCost   uint64         `arg:"--ristretto_avg_cost,env:RISTRETTO_AVG_COST" json:"ristretto_avg_cost,omitempty" default:"100"`
	Dev                bool           `arg:"--dev" default:"true" json:"dev,omitempty"`
}

func (args NitrousArgs) Valid() error {
	// TODO: implement
	return nil
}

type KafkaConsumerFactory func(libkafka.ConsumerConfig) (libkafka.FConsumer, error)

type Nitrous struct {
	PlaneID              ftypes.RealmID
	Logger               *zap.Logger
	Clock                clock.Clock
	Store                hangar.Hangar
	KafkaConsumerFactory KafkaConsumerFactory
}

func CreateFromArgs(args NitrousArgs) (Nitrous, error) {
	scope := resource.NewPlaneScope(args.PlaneID)

	log.Print("Creating logger")
	var logger *zap.Logger
	var err error
	if args.Dev {
		logger, err = zap.NewDevelopment()
	} else {
		config := zap.NewProductionConfig()
		config.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
		logger, err = config.Build(
			zap.AddCaller(),
			zap.AddStacktrace(zap.ErrorLevel),
		)
	}
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to construct logger: %w", err)
	}
	logger = logger.With(zap.Uint32("plane_id", args.PlaneID.Value()))

	// Initialize kafka consumer factory.
	consumerFactory := func(config libkafka.ConsumerConfig) (libkafka.FConsumer, error) {
		kafkaConsumerConfig := libkafka.RemoteConsumerConfig{
			BootstrapServer: args.KafkaServer,
			Username:        args.KafkaUsername,
			Password:        args.KafkaPassword,
			ConsumerConfig:  config,
		}
		kafkaConsumer, err := kafkaConsumerConfig.Materialize()
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		return kafkaConsumer.(libkafka.FConsumer), nil
	}

	// Initialize layered storage.
	db, err := db.NewHangar(scope.ID(), args.BadgerDir, args.BadgerBlockCacheMB<<20, encoders.Default())
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to create badger db: %w", err)
	}
	cache, err := cache.NewHangar(scope.ID(), args.RistrettoMaxCost, args.RistrettoAvgCost, encoders.Default())
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to create cache: %w", err)
	}
	layered := layered.NewHangar(scope.ID(), cache, db)

	return Nitrous{
		PlaneID:              scope.ID(),
		KafkaConsumerFactory: consumerFactory,
		Clock:                clock.New(),
		Logger:               logger,
		Store:                layered,
	}, nil
}
