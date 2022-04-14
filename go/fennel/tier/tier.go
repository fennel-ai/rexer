package tier

import (
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"time"

	"fennel/db"
	"fennel/fbadger"
	"fennel/kafka"
	"fennel/lib/cache"
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	"fennel/modelstore"
	"fennel/pcache"
	"fennel/redis"
	"fennel/resource"
	"fennel/s3"
	"fennel/sagemaker"

	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TierArgs struct {
	sagemaker.SagemakerArgs `json:"sagemaker_._sagemaker_args"`
	ModelStoreS3Bucket      string `arg:"--model-store-s3-bucket,env:MODEL_STORE_S3_BUCKET,help:Model Store S3 bucket name"`
	ModelStoreEndpointName  string `arg:"--model-store-endpoint,env:MODEL_STORE_ENDPOINT,help:Model Store endpoint name"`

	KafkaServer   string         `arg:"--kafka-server,env:KAFKA_SERVER_ADDRESS" json:"kafka_server,omitempty"`
	KafkaUsername string         `arg:"--kafka-user,env:KAFKA_USERNAME" json:"kafka_username,omitempty"`
	KafkaPassword string         `arg:"--kafka-password,env:KAFKA_PASSWORD" json:"kafka_password,omitempty"`
	MysqlHost     string         `arg:"--mysql-host,env:MYSQL_SERVER_ADDRESS" json:"mysql_host,omitempty"`
	MysqlDB       string         `arg:"--mysql-db,env:MYSQL_DATABASE_NAME" json:"mysql_db,omitempty"`
	MysqlUsername string         `arg:"--mysql-user,env:MYSQL_USERNAME" json:"mysql_username,omitempty"`
	MysqlPassword string         `arg:"--mysql-password,env:MYSQL_PASSWORD" json:"mysql_password,omitempty"`
	TierID        ftypes.RealmID `arg:"--tier-id,env:TIER_ID" json:"tier_id,omitempty"`
	RedisServer   string         `arg:"--redis-server,env:REDIS_SERVER_ADDRESS" json:"redis_server,omitempty"`
	CachePrimary  string         `arg:"--cache-primary,env:CACHE_PRIMARY" json:"cache_primary,omitempty"`
	CacheReplica  string         `arg:"--cache-replica,env:CACHE_REPLICA" json:"cache_replica,omitempty"`
	Dev           bool           `arg:"--dev" default:"true" json:"dev,omitempty"`
	BadgerDir     string         `arg:"--badger_dir,env:BADGER_DIR" json:"badger_dir,omitempty"`
}

type KafkaConsumerCreator func(string, string, string) (kafka.FConsumer, error)

func (args TierArgs) Valid() error {
	missingFields := make([]string, 0)
	if args.KafkaServer == "" {
		missingFields = append(missingFields, "KAFKA_SERVER")
	}
	if args.KafkaUsername == "" {
		missingFields = append(missingFields, "KAFKA_USERNAME")
	}
	if args.KafkaPassword == "" {
		missingFields = append(missingFields, "KAFKA_PASSWORD")
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
	if args.RedisServer == "" {
		missingFields = append(missingFields, "REDIS_SERVER_ADDRESS")
	}
	if args.CachePrimary == "" {
		missingFields = append(missingFields, "CACHE_PRIMARY")
	}
	if args.TierID == 0 {
		missingFields = append(missingFields, "TIER_ID")
	}
	if args.BadgerDir == "" {
		missingFields = append(missingFields, "BADGER_DIR")
	}
	if args.ModelStoreS3Bucket == "" {
		missingFields = append(missingFields, "MODEL_STORE_S3_BUCKET")
	}
	if args.ModelStoreEndpointName == "" {
		missingFields = append(missingFields, "MODEL_STORE_ENDPOINT")
	}
	if len(missingFields) > 0 {
		return fmt.Errorf("missing fields: %s", strings.Join(missingFields, ", "))
	}
	return nil
}

/*
	Design doc:https://coda.io/d/Fennel-Engineering-Guidelines_d1vISIa2cbh/Tier-Data-Plane-abstraction_su91h#_luTxV
*/

type Tier struct {
	ID               ftypes.RealmID
	DB               db.Connection
	Redis            redis.Client
	Cache            cache.Cache
	PCache           pcache.PCache
	Producers        map[string]kafka.FProducer
	Clock            clock.Clock
	Logger           *zap.Logger
	NewKafkaConsumer KafkaConsumerCreator
	S3Client         s3.Client
	SagemakerClient  sagemaker.SMClient
	ModelStore       *modelstore.ModelStore
	Badger           fbadger.DB
}

func CreateFromArgs(args *TierArgs) (tier Tier, err error) {
	tierID := args.TierID
	scope := resource.NewTierScope(tierID)

	log.Print("Connecting to mysql")
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
	log.Print("Connecting to redis")
	redisConfig := redis.ClientConfig{
		Addr:      args.RedisServer,
		TLSConfig: &tls.Config{},
		Scope:     scope,
	}
	redisClient, err := redisConfig.Materialize()
	if err != nil {
		return tier, fmt.Errorf("failed to create redis client: %v", err)
	}

	log.Print("Connecting to cache")
	cacheClientConfig := redis.ClientConfig{
		Addr:      args.CachePrimary,
		TLSConfig: &tls.Config{},
		Scope:     scope,
	}
	cacheClient, err := cacheClientConfig.Materialize()
	if err != nil {
		return tier, fmt.Errorf("failed to create cache client: %v", err)
	}

	log.Print("Creating process-level cache")
	// Capacity: 2 GB
	// Expected size of item: 64 bytes
	pCache, err := pcache.NewPCache(1<<31, 1<<6)
	if err != nil {
		return tier, fmt.Errorf("failed to create process-level cache: %v", err)
	}

	// Start a goroutine to periodically record various connection stats.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for ; true; <-ticker.C {
			db.RecordConnectionStats(sqlConn.(db.Connection))
			redis.RecordConnectionStats("redis", redisClient.(redis.Client))
			redis.RecordConnectionStats("elasticache", cacheClient.(redis.Client))
			pcache.RecordStats("pcache", pCache)
		}
	}()

	log.Print("Creating kafka producer")
	producers, err := CreateKafka(scope, args.KafkaServer, args.KafkaUsername, args.KafkaPassword)
	if err != nil {
		return tier, err
	}

	log.Print("Creating kafka consumer")
	consumerCreator := func(topic, groupID, offsetPolicy string) (kafka.FConsumer, error) {
		kafkaConsumerConfig := kafka.RemoteConsumerConfig{
			BootstrapServer: args.KafkaServer,
			Username:        args.KafkaUsername,
			Password:        args.KafkaPassword,
			GroupID:         groupID,
			Topic:           scope.PrefixedName(topic),
			OffsetPolicy:    offsetPolicy,
			Scope:           scope,
		}
		kafkaConsumer, err := kafkaConsumerConfig.Materialize()
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		return kafkaConsumer.(kafka.FConsumer), nil
	}

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
	logger = logger.With(zap.Uint32("tier_id", args.TierID.Value()))

	smclient, err := sagemaker.NewClient(args.SagemakerArgs)
	if err != nil {
		return tier, fmt.Errorf("failed to create sagemaker client: %v", err)
	}

	// TODO - get region from own args
	s3Args := s3.S3Args{Region: args.SagemakerArgs.Region}
	s3client := s3.NewClient(s3Args)

	modelStore := modelstore.NewModelStore(args.ModelStoreS3Bucket, args.ModelStoreEndpointName)

	log.Print("Creating badger")
	opts := badger.DefaultOptions(args.BadgerDir)
	// only log warnings and errors
	opts = opts.WithLoggingLevel(badger.WARNING)
	badgerConf := fbadger.Config{
		Opts:  opts,
		Scope: scope,
	}
	bdb, err := badgerConf.Materialize()
	if err != nil {
		return tier, fmt.Errorf("failed to create badger: %v", err)
	}

	return Tier{
		DB:               sqlConn.(db.Connection),
		Redis:            redisClient.(redis.Client),
		Producers:        producers,
		Clock:            clock.Unix{},
		ID:               tierID,
		Logger:           logger,
		Cache:            redis.NewCache(cacheClient.(redis.Client)),
		PCache:           pCache,
		NewKafkaConsumer: consumerCreator,
		SagemakerClient:  smclient,
		S3Client:         s3client,
		ModelStore:       modelStore,
		Badger:           bdb.(fbadger.DB),
	}, nil
}

func CreateKafka(scope resource.Scope, server, username, password string) (map[string]kafka.FProducer, error) {
	producers := make(map[string]kafka.FProducer)
	for _, topic := range kafka.ALL_TOPICS {
		kafkaProducerConfig := kafka.RemoteProducerConfig{
			BootstrapServer: server,
			Username:        username,
			Password:        password,
			Topic:           scope.PrefixedName(topic),
			Scope:           scope,
		}
		kafkaProducer, err := kafkaProducerConfig.Materialize()
		if err != nil {
			return nil, fmt.Errorf("failed to crate kafka producer: %v", err)
		}
		producers[topic] = kafkaProducer.(kafka.FProducer)

	}
	return producers, nil
}
