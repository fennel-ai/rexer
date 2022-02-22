package tier

import (
	"crypto/tls"
	"fmt"
	"strings"

	"fennel/db"
	"fennel/kafka"
	"fennel/lib/cache"
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	"fennel/redis"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TierArgs struct {
	KafkaServer   string        `arg:"--kafka-server,env:KAFKA_SERVER_ADDRESS"`
	KafkaUsername string        `arg:"--kafka-user,env:KAFKA_USERNAME"`
	KafkaPassword string        `arg:"--kafka-password,env:KAFKA_PASSWORD"`
	MysqlHost     string        `arg:"--mysql-host,env:MYSQL_SERVER_ADDRESS"`
	MysqlDB       string        `arg:"--mysql-db,env:MYSQL_DATABASE_NAME"`
	MysqlUsername string        `arg:"--mysql-user,env:MYSQL_USERNAME"`
	MysqlPassword string        `arg:"--mysql-password,env:MYSQL_PASSWORD"`
	TierID        ftypes.TierID `arg:"--tier-id,env:TIER_ID"`
	RedisServer   string        `arg:"--redis-server,env:REDIS_SERVER_ADDRESS"`
	CachePrimary  string        `arg:"--cache-primary,env:CACHE_PRIMARY"`
	CacheReplica  string        `arg:"--cache-replica,env:CACHE_REPLICA"`
	Dev           bool          `arg:"--dev" default:"true"`
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
	if args.TierID == 0 {
		missingFields = append(missingFields, "TIER_ID")
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
	ID               ftypes.TierID
	DB               db.Connection
	Redis            redis.Client
	Cache            cache.Cache
	Producers        map[string]kafka.FProducer
	Clock            clock.Clock
	Logger           *zap.Logger
	NewKafkaConsumer KafkaConsumerCreator
}

func CreateFromArgs(args *TierArgs) (tier Tier, err error) {
	tierID := args.TierID

	mysqlConfig := db.MySQLConfig{
		Host:     args.MysqlHost,
		DBname:   args.MysqlDB,
		Username: args.MysqlUsername,
		Password: args.MysqlPassword,
		Schema:   Schema,
	}
	sqlConn, err := mysqlConfig.Materialize(tierID)
	if err != nil {
		return tier, fmt.Errorf("failed to connect with mysql: %v", err)
	}

	redisConfig := redis.ClientConfig{
		Addr:      args.RedisServer,
		TLSConfig: &tls.Config{},
	}
	redisClient, err := redisConfig.Materialize(tierID)
	if err != nil {
		return tier, fmt.Errorf("failed to create redis client: %v", err)
	}

	cacheClientConfig := redis.ClientConfig{
		Addr:      args.CachePrimary,
		TLSConfig: &tls.Config{},
	}
	cacheClient, err := cacheClientConfig.Materialize(tierID)
	if err != nil {
		return tier, fmt.Errorf("failed to create cache client: %v", err)
	}
	producers, err := CreateKafka(tierID, args.KafkaServer, args.KafkaUsername, args.KafkaPassword)
	if err != nil {
		return tier, err
	}

	consumerCreator := func(topic, groupID, offsetPolicy string) (kafka.FConsumer, error) {
		kafkaConsumerConfig := kafka.RemoteConsumerConfig{
			BootstrapServer: args.KafkaServer,
			Username:        args.KafkaUsername,
			Password:        args.KafkaPassword,
			GroupID:         groupID,
			Topic:           topic,
			OffsetPolicy:    offsetPolicy,
		}
		kafkaConsumer, err := kafkaConsumerConfig.Materialize(args.TierID)
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		return kafkaConsumer.(kafka.FConsumer), nil
	}

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

	return Tier{
		DB:               sqlConn.(db.Connection),
		Redis:            redisClient.(redis.Client),
		Producers:        producers,
		Clock:            clock.Unix{},
		ID:               tierID,
		Logger:           logger,
		Cache:            redis.NewCache(cacheClient.(redis.Client)),
		NewKafkaConsumer: consumerCreator,
	}, nil
}

func CreateKafka(tierID ftypes.TierID, server, username, password string) (map[string]kafka.FProducer, error) {
	producers := make(map[string]kafka.FProducer)
	for _, topic := range kafka.ALL_TOPICS {
		kafkaProducerConfig := kafka.RemoteProducerConfig{
			BootstrapServer: server,
			Username:        username,
			Password:        password,
			Topic:           topic,
		}
		kafkaProducer, err := kafkaProducerConfig.Materialize(tierID)
		if err != nil {
			return nil, fmt.Errorf("failed to crate kafka producer: %v", err)
		}
		producers[topic] = kafkaProducer.(kafka.FProducer)

	}
	return producers, nil
}
