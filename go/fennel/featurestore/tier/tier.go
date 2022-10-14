package tier

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"fennel/airbyte"
	"fennel/lib/instancemetadata"

	"github.com/Unleash/unleash-client-go/v3"

	"fennel/db"
	libkafka "fennel/kafka"
	"fennel/lib/aggregate"
	"fennel/lib/cache"
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	libnitrous "fennel/lib/nitrous"
	"fennel/lib/timer"
	unleashlib "fennel/lib/unleash"
	nitrous "fennel/nitrous/client"
	"fennel/pcache"
	"fennel/resource"
	"fennel/s3"
	"github.com/samber/mo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TierArgs struct {
	s3.S3Args        `json:"s3_._s3_args"`
	timer.TracerArgs `json:"tracer_._tracer_args"`

	Region string `arg:"--aws-region,env:AWS_REGION" json:"aws_region,omitempty"`
	// MSK configuration
	MskKafkaServer   string `arg:"--msk-kafka-server,env:MSK_KAFKA_SERVER_ADDRESS" json:"msk_kafka_server,omitempty"`
	MskKafkaUsername string `arg:"--msk-kafka-user,env:MSK_KAFKA_USERNAME" json:"msk_kafka_username,omitempty"`
	MskKafkaPassword string `arg:"--msk-kafka-password,env:MSK_KAFKA_PASSWORD" json:"msk_kafka_password,omitempty"`

	MysqlHost        string         `arg:"--mysql-host,env:MYSQL_SERVER_ADDRESS" json:"mysql_host,omitempty"`
	MysqlDB          string         `arg:"--mysql-db,env:MYSQL_DATABASE_NAME" json:"mysql_db,omitempty"`
	MysqlUsername    string         `arg:"--mysql-user,env:MYSQL_USERNAME" json:"mysql_username,omitempty"`
	MysqlPassword    string         `arg:"--mysql-password,env:MYSQL_PASSWORD" json:"mysql_password,omitempty"`
	TierID           ftypes.RealmID `arg:"--tier-id,env:TIER_ID" json:"tier_id,omitempty"`
	PlaneID          ftypes.RealmID `arg:"--plane-id,env:PLANE_ID" json:"plane_id,omitempty"`
	RequestLimit     int64          `arg:"--request-limit,env:REQUEST_LIMIT" default:"-1" json:"request_limit,omitempty"`
	NitrousServer    string         `arg:"--nitrous-server,env:NITROUS_SERVER_ADDRESS" json:"nitrous_server,omitempty"`
	BinlogPartitions uint32         `arg:"--binlog-partitions,env:BINLOG_PARTITIONS" json:"binlog_partitions,omitempty"`
	CachePrimary     string         `arg:"--cache-primary,env:CACHE_PRIMARY" json:"cache_primary,omitempty"`
	CacheReplica     string         `arg:"--cache-replica,env:CACHE_REPLICA" json:"cache_replica,omitempty"`
	Dev              bool           `arg:"--dev" default:"true" json:"dev,omitempty"`
	OfflineAggBucket string         `arg:"--offline-agg-bucket,env:OFFLINE_AGG_BUCKET" json:"offline_agg_bucket,omitempty"`
	UnleashEndpoint  string         `arg:"--unleash-endpoint,env:UNLEASH_ENDPOINT" json:"unleash_endpoint,omitempty"`
	AirbyteServer    string         `arg:"--airbyte-server,env:AIRBYTE_SERVER_ADDRESS" json:"airbyte_server,omitempty"`

	InstanceMetadataServiceAddr string `arg:"--instance-metadata-service-addr,env:INSTANCE_METADATA_SERVICE_ADDR" json:"instance_metadata_service_addr,omitempty"`
}

type KafkaConsumerCreator func(libkafka.ConsumerConfig) (libkafka.FConsumer, error)

func (args TierArgs) Valid() error {
	missingFields := make([]string, 0)
	if args.MskKafkaServer == "" {
		missingFields = append(missingFields, "MSK_KAFKA_SERVER")
	}
	if args.MskKafkaUsername == "" {
		missingFields = append(missingFields, "MSK_KAFKA_USERNAME")
	}
	if args.MskKafkaPassword == "" {
		missingFields = append(missingFields, "MSK_KAFKA_PASSWORD")
	}
	if args.MysqlHost == "" {
		missingFields = append(missingFields, "MYSQL_SERVER_ADDRESS")
	}
	if args.MysqlDB == "" {
		missingFields = append(missingFields, "MYSQL_DATABASE_NAME")
	}
	if args.MysqlUsername == "" {
		missingFields = append(missingFields, "MYSQL_USERNAME")
	}
	if args.MysqlPassword == "" {
		missingFields = append(missingFields, "MYSQL_PASSWORD")
	}
	if args.CachePrimary == "" {
		missingFields = append(missingFields, "CACHE_PRIMARY")
	}
	if args.TierID == 0 {
		missingFields = append(missingFields, "TIER_ID")
	}
	if args.PlaneID == 0 {
		missingFields = append(missingFields, "PLANE_ID")
	}

	// TODO(mohit): make this a required argument
	if args.MskKafkaServer != "" {
		if args.MskKafkaUsername == "" {
			missingFields = append(missingFields, "MSK_KAFKA_USERNAME")
		}
		if args.MskKafkaPassword == "" {
			missingFields = append(missingFields, "MSK_KAFKA_PASSWORD")
		}
	}
	return nil
}

/*
	Note: For old tier, might be outdated
	Design doc:https://coda.io/d/Fennel-Engineering-Guidelines_d1vISIa2cbh/Tier-Data-Plane-abstraction_su91h#_luTxV
*/

type Tier struct {
	ID ftypes.RealmID
	DB db.Connection
	// Elastic Cache ( external service & higher level cache with more capacity with LRU eviction )
	Cache            cache.Cache
	Producers        map[string]libkafka.FProducer
	Clock            clock.Clock
	Logger           *zap.Logger
	NewKafkaConsumer KafkaConsumerCreator
	S3Client         s3.Client
	//GlueClient        glue.GlueClient
	//EventBridgeClient eventbridge.Client
	AirbyteClient mo.Option[airbyte.Client]
	//SagemakerClient   sagemaker.SMClient
	//MilvusClient      mo.Option[milvus.Client]
	NitrousClient mo.Option[nitrous.NitrousClient]
	//ModelStore        *modelstore.ModelStore
	Args TierArgs
	// In-process caches for the tier, has very short TTL ( order of minutes )
	PCache       pcache.PCache
	RequestLimit int64
}

func CreateFromArgs(args *TierArgs) (tier Tier, err error) {
	tierID := args.TierID
	scope := resource.NewTierScope(tierID)

	// First, create a structured logger that we can then use in other places.
	log.Print("Creating logger")
	var logger *zap.Logger
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
		return tier, fmt.Errorf("failed to construct logger: %v", err)
	}
	_ = zap.ReplaceGlobals(logger)

	var azId mo.Option[string]
	if len(args.InstanceMetadataServiceAddr) > 0 {
		// AvailabilityZoneId is only enabled for prod since it talks to the EC2 instance metadata service to fetch
		// the AZ it is running in
		id, err := instancemetadata.GetAvailabilityZoneId(args.InstanceMetadataServiceAddr)
		if err != nil {
			return Tier{}, err
		}
		azId = mo.Some(id)
	}

	logger = logger.With(zap.Uint32("tier_id", args.TierID.Value()))

	logger.Info("Connecting to mysql")
	mysqlConfig := db.MySQLConfig{
		Host:     args.MysqlHost,
		DBname:   scope.PrefixedName(args.MysqlDB),
		Username: args.MysqlUsername,
		Password: args.MysqlPassword,
		Schema:   Schema,
	}
	sqlConn, err := mysqlConfig.Materialize()
	if err != nil {
		return tier, fmt.Errorf("failed to connect with mysql: %v", err)
	}

	logger.Info("Creating process-level cache")
	// Capacity: 2 GB
	// Expected size of item: 128 bytes
	pCache, err := pcache.NewPCache(1<<31, 1<<7)
	if err != nil {
		return tier, fmt.Errorf("failed to create process-level cache: %v", err)
	}

	// Start a goroutine to periodically record various resource level stats.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for ; true; <-ticker.C {
			db.RecordConnectionStats(sqlConn.(db.Connection))
			pcache.RecordStats("pcache", pCache)
		}
	}()

	logger.Info("Creating kafka producers")
	producers, err := CreateKafka(tierID, args.PlaneID, args.MskKafkaServer, args.MskKafkaUsername, args.MskKafkaPassword, libkafka.SaslScramSha512Mechanism, libkafka.ALL_TOPICS)
	if err != nil {
		return tier, fmt.Errorf("failed to create producers for confluent based kafka topics: %v", err)
	}

	logger.Info("Creating kafka consumer factory")
	consumerCreator := func(config libkafka.ConsumerConfig) (libkafka.FConsumer, error) {
		kafkaConsumerConfig := libkafka.RemoteConsumerConfig{
			ConsumerConfig:  config,
			BootstrapServer: args.MskKafkaServer,
			Username:        args.MskKafkaUsername,
			Password:        args.MskKafkaPassword,
			SaslMechanism:   libkafka.SaslScramSha512Mechanism,
			AzId:            azId,
		}
		kafkaConsumer, err := kafkaConsumerConfig.Materialize()
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		return kafkaConsumer.(libkafka.FConsumer), nil
	}

	nitrousClient := mo.None[nitrous.NitrousClient]()
	if args.NitrousServer != "" {
		logger.Info("Connecting to nitrous")
		binlogProducer, ok := producers[libnitrous.BINLOG_KAFKA_TOPIC]
		if !ok {
			return tier, fmt.Errorf("failed to create nitrous client; Binlog kafka topic not configured")
		}
		reqslogProducer, ok := producers[libnitrous.REQS_KAFKA_TOPIC]
		if !ok {
			return tier, fmt.Errorf("failed to create nitrous client; Reqslog kafka topic not configured")
		}
		aggregateConfProducer, ok := producers[libnitrous.AGGR_CONF_KAFKA_TOPIC]
		if !ok {
			return tier, fmt.Errorf("failed to create nitrous client; aggregate_conf topic not configured")
		}
		nitrousConfig := nitrous.NitrousClientConfig{
			TierID:                args.TierID,
			ServerAddr:            args.NitrousServer,
			BinlogProducer:        binlogProducer,
			BinlogPartitions:      args.BinlogPartitions,
			ReqsLogProducer:       reqslogProducer,
			AggregateConfProducer: aggregateConfProducer,
		}
		client, err := nitrousConfig.Materialize()
		if err != nil {
			return tier, fmt.Errorf("failed to create nitrous client: %w", err)
		}
		nitrousClient = mo.Some(client.(nitrous.NitrousClient))
	}

	airbyteClient := mo.None[airbyte.Client]()
	if args.AirbyteServer != "" {
		logger.Info("Connecting to airbyte")
		// setup the kafka topic always in the MSK cluster
		client, err := airbyte.NewClient(args.AirbyteServer, tierID, airbyte.KafkaCredentials{
			Username: args.MskKafkaUsername,
			Password: args.MskKafkaPassword,
			Server:   args.MskKafkaServer,
		})
		if err != nil {
			return tier, fmt.Errorf("failed to create airbyte client: %v", err)
		}
		airbyteClient = mo.Some(client)
	}

	logger.Info("Creating AWS clients for S3")
	s3client := s3.NewClient(args.S3Args)

	// initialize unleash endpoint
	//
	// currently make the initialization optional, we should setup local developer infrastructure for this to work
	// (e.g. for UT and integration tests, this could be a mock).
	//
	// otherwise, we inject a fake unleash which returns false by default.
	//
	// TODO(mohit): Create application infra for unleash which would just inject the fake for testing and
	// use the real instance in production.
	if len(args.UnleashEndpoint) > 0 {
		if err := unleash.Initialize(
			unleash.WithListener(&unleash.DebugListener{}),
			// project name for unpaid self-hosted instances is `default` by-default
			unleash.WithProjectName("default"),
			// TODO: Consider passing this name as EnvVar to have different services different names for granular
			// request logging
			unleash.WithAppName("fennel-servers"),
			// disable reporting metrics, they are currently of no use right now
			unleash.WithDisableMetrics(true),
			// TODO: Consider setting environment (default v/s staging v/s prod) for testing out behaviors on
			// staging first and prod later
			unleash.WithEnvironment("production"),
			unleash.WithUrl(args.UnleashEndpoint),
		); err != nil {
			return tier, fmt.Errorf("creating tier ")
		}
	} else {
		faker := unleashlib.NewFakeUnleash()
		if err := unleash.Initialize(unleash.WithListener(&unleash.DebugListener{}),
			unleash.WithAppName("local-tier"),
			unleash.WithUrl(faker.Url())); err != nil {
			return tier, fmt.Errorf("failed created fake unleash")
		}
	}

	// Setup tracer provider (which exports remotely) if an endpoint is defined. Otherwise a default tracer is used.
	if len(args.OtlpEndpoint) > 0 {
		err = timer.InitProvider(args.OtlpEndpoint, timer.PathSampler{SamplingRatio: args.SamplingRatio})
		if err != nil {
			panic(err)
		}
	}

	return Tier{
		DB:               sqlConn.(db.Connection),
		Producers:        producers,
		Clock:            clock.Unix{},
		ID:               tierID,
		Logger:           logger,
		PCache:           pCache,
		NewKafkaConsumer: consumerCreator,
		NitrousClient:    nitrousClient,
		S3Client:         s3client,
		AirbyteClient:    airbyteClient,
		Args:             *args,
		RequestLimit:     args.RequestLimit,
	}, nil
}

func CreateKafka(tierID, planeID ftypes.RealmID, server, username, password, saslMechanism string, topics []libkafka.TopicConf) (map[string]libkafka.FProducer, error) {
	producers := make(map[string]libkafka.FProducer)
	for _, topic := range topics {
		var scope resource.Scope
		switch topic.Scope.(type) {
		case resource.TierScope:
			scope = resource.NewTierScope(tierID)
		case resource.PlaneScope:
			scope = resource.NewPlaneScope(planeID)
		default:
			return nil, fmt.Errorf("unknown scope type: %T", topic.Scope)
		}
		kafkaProducerConfig := libkafka.RemoteProducerConfig{
			BootstrapServer: server,
			Username:        username,
			Password:        password,
			SaslMechanism:   saslMechanism,
			Topic:           topic.Topic,
			Scope:           scope,
			Configs:         topic.PConfigs,
		}
		kafkaProducer, err := kafkaProducerConfig.Materialize()
		if err != nil {
			return nil, fmt.Errorf("failed to crate kafka producer: %v", err)
		}
		producers[topic.Topic] = kafkaProducer.(libkafka.FProducer)
	}
	return producers, nil
}

// populateAggregateCache retrieves all active aggregates and sets them on the cache
//
// NOTE: this works on best effort basis i.e. does not return an error and may not update the cache at all
func populateAggregateCache(cache *sync.Map, sqlConn resource.Resource, logger *zap.Logger) {
	// we do not rely on the aggregate controller here to primarily maintain dependency hierarchy
	// (to avoid cyclic dependencies). Tier is a process level package (and resource) and should ideally not
	// depend on packages other than other utility or third-party libraries
	var aggregates []aggregate.AggregateSer
	err := sqlConn.(db.Connection).SelectContext(context.Background(), &aggregates, `SELECT * FROM aggregate_config WHERE active = TRUE`)
	if err != nil {
		logger.Warn("failed to populate the aggregate cache with active aggregates", zap.Error(err))
		return
	}
	for i := range aggregates {
		agg, err := aggregates[i].ToAggregate()
		if err != nil {
			logger.Warn("failed to convert aggregate def", zap.String("name", string(aggregates[i].Name)), zap.Error(err))
			continue
		}
		cache.Store(agg.Name, agg)
	}
}
