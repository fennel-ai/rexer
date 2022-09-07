package nitrous

import (
	"fennel/lib/instancemetadata"
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/samber/mo"

	"fennel/hangar"
	"fennel/hangar/db"
	"fennel/hangar/encoders"
	"fennel/hangar/layered"
	"fennel/hangar/mem"
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

	// Initialize layered storage.
	badgerOpts := badger.DefaultOptions(args.BadgerDir)
	badgerOpts = badgerOpts.WithLogger(db.NewLogger(zap.L()))
	badgerOpts = badgerOpts.WithValueThreshold(1 << 10 /* 1 KB */)
	badgerOpts = badgerOpts.WithBlockSize(4 << 10 /* 4 KB */)
	badgerOpts = badgerOpts.WithNumCompactors(2)
	badgerOpts = badgerOpts.WithCompactL0OnClose(true)
	// TODO: Make index cache size a flag.
	badgerOpts = badgerOpts.WithIndexCacheSize(16 << 30 /* 16 GB */)
	badgerOpts = badgerOpts.WithMemTableSize(256 << 20 /* 256 MB */)
	if args.Compress {
		badgerOpts = badgerOpts.WithCompression(options.ZSTD)
		badgerOpts = badgerOpts.WithBlockCacheSize(args.BadgerBlockCacheMB << 20)
	} else {
		badgerOpts = badgerOpts.WithCompression(options.None)
		badgerOpts = badgerOpts.WithBlockCacheSize(0)
	}
	badgerOpts = badgerOpts.WithMetricsEnabled(false)
	db, err := db.NewHangar(scope.ID(), badgerOpts, encoders.Default())
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to create badger db: %w", err)
	}
	cache, err := mem.NewHangar(scope.ID(), 64, encoders.Default())
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to create cache: %w", err)
	}
	layered := layered.NewHangar(scope.ID(), cache, db)

	return Nitrous{
		PlaneID:              scope.ID(),
		Identity:             args.Identity,
		KafkaConsumerFactory: consumerFactory,
		Clock:                clock.New(),
		Store:                layered,
	}, nil
}
