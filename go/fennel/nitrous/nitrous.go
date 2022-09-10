package nitrous

import (
	"fennel/gravel"
	"fennel/lib/instancemetadata"
	"fmt"
	"log"

	"github.com/samber/mo"

	"fennel/hangar"
	"fennel/hangar/encoders"
	gravelDB "fennel/hangar/gravel"
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
	MskKafkaServer     string         `arg:"--msk-kafka-server,env:MSK_KAFKA_SERVER_ADDRESS" json:"msk_kafka_server,omitempty"`
	MskKafkaUsername   string         `arg:"--msk-kafka-user,env:MSK_KAFKA_USERNAME" json:"msk_kafka_username,omitempty"`
	MskKafkaPassword   string         `arg:"--msk-kafka-password,env:MSK_KAFKA_PASSWORD" json:"msk_kafka_password,omitempty"`
	BadgerDir          string         `arg:"--badger_dir,env:BADGER_DIR" json:"badger_dir,omitempty"`
	PebbleDir          string         `arg:"--pebble_dir,env:PEBBLE_DIR" json:"pebble_dir,omitempty"`
	GravelDir          string         `arg:"--gravel_dir,env:GRAVEL_DIR" json:"gravel_dir,omitempty"`
	BadgerBlockCacheMB int64          `arg:"--badger_block_cache_mb,env:BADGER_BLOCK_CACHE_MB" json:"badger_block_cache_mb,omitempty"`
	RistrettoMaxCost   uint64         `arg:"--ristretto_max_cost,env:RISTRETTO_MAX_COST" json:"ristretto_max_cost,omitempty"`
	RistrettoAvgCost   uint64         `arg:"--ristretto_avg_cost,env:RISTRETTO_AVG_COST" json:"ristretto_avg_cost,omitempty" default:"1000"`

	InstanceMetadataServiceAddr string `arg:"--instance-metadata-service-addr,env:INSTANCE_METADATA_SERVICE_ADDR" json:"instance_metadata_service_Addr,omitempty"`

	// Identity should be unique for each instance of nitrous. The IDENTITY environment
	// variable should be unique for each replica of a StatefulSet in k8s.
	Identity string `arg:"--identity,env:IDENTITY" json:"identity" default:"localhost"`
	// Flag to enable data compression.
	Compress bool `arg:"--compress,env:COMPRESS" json:"compress" default:"false"`
	Dev      bool `arg:"--dev" default:"true" json:"dev,omitempty"`
}

func (args NitrousArgs) Valid() error {
	// TODO: implement
	return nil
}

type KafkaConsumerFactory func(libkafka.ConsumerConfig) (libkafka.FConsumer, error)

type Nitrous struct {
	PlaneID              ftypes.RealmID
	Identity             string
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

	azId := mo.None[string]()
	if len(args.InstanceMetadataServiceAddr) > 0 {
		id, err := instancemetadata.GetAvailabilityZoneId(args.InstanceMetadataServiceAddr)
		if err != nil {
			return Nitrous{}, fmt.Errorf("failed to get AZ Id: %v", err)
		}
		azId = mo.Some(id)
	}

	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to construct logger: %w", err)
	}
	logger = logger.With(
		zap.Uint32("plane", args.PlaneID.Value()),
		zap.String("identity", args.Identity),
	)
	_ = zap.ReplaceGlobals(logger)

	// Initialize kafka consumer factory.
	consumerFactory := func(config libkafka.ConsumerConfig) (libkafka.FConsumer, error) {
		kafkaConsumerConfig := libkafka.RemoteConsumerConfig{
			BootstrapServer: args.MskKafkaServer,
			Username:        args.MskKafkaUsername,
			Password:        args.MskKafkaPassword,
			SaslMechanism:   libkafka.SaslScramSha512Mechanism,
			ConsumerConfig:  config,
			AzId:            azId,
		}
		kafkaConsumer, err := kafkaConsumerConfig.Materialize()
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		return kafkaConsumer.(libkafka.FConsumer), nil
	}

	gravalOps := gravel.DefaultOptions().WithDirname(args.GravelDir)
	gravelDb, err := gravelDB.NewHangar(scope.ID(), args.GravelDir, &gravalOps, encoders.Default())
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to create gravel db: %w", err)
	}

	return Nitrous{
		PlaneID:              scope.ID(),
		Identity:             args.Identity,
		KafkaConsumerFactory: consumerFactory,
		Clock:                clock.New(),
		Store:                gravelDb,
	}, nil
}
