package tier

import (
	"crypto/tls"
	"fmt"

	"fennel/db"
	"fennel/kafka"
	"fennel/lib/cache"
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	"fennel/redis"
)

// Flags for the aggreagator server.
type PlaneArgs struct {
	KafkaServer   string `arg:"--kafka-server,env:KAFKA_SERVER_ADDRESS"`
	KafkaUsername string `arg:"--kafka-user,env:KAFKA_USERNAME"`
	KafkaPassword string `arg:"--kafka-password,env:KAFKA_PASSWORD"`

	MysqlHost     string `arg:"--mysql-host,env:MYSQL_SERVER_ADDRESS"`
	MysqlDB       string `arg:"--mysql-db,env:MYSQL_DATABASE_NAME"`
	MysqlUsername string `arg:"--mysql-user,env:MYSQL_USERNAME"`
	MysqlPassword string `arg:"--mysql-password,env:MYSQL_PASSWORD"`

	RedisServer string `arg:"--redis-server,env:REDIS_SERVER_ADDRESS"`
}

/*
	Design doc:https://coda.io/d/Fennel-Engineering-Guidelines_d1vISIa2cbh/Tier-Data-Plane-abstraction_su91h#_luTxV
*/

type Tier struct {
	ID             ftypes.TierID
	CustID         ftypes.CustID
	DB             db.Connection
	Redis          redis.Client
	Cache          cache.Cache
	ActionProducer kafka.FProducer
	ActionConsumer kafka.FConsumer
	Clock          clock.Clock
}

func CreateFromArgs(args *PlaneArgs) (plane Tier, err error) {
	tierID := ftypes.TierID(1)

	mysqlConfig := db.MySQLConfig{
		Host:     args.MysqlHost,
		DBname:   args.MysqlDB,
		Username: args.MysqlUsername,
		Password: args.MysqlPassword,
		TierID:   tierID,
	}
	sqlConn, err := mysqlConfig.Materialize()
	if err != nil {
		return plane, fmt.Errorf("failed to connect with mysql: %v", err)
	}

	redisConfig := redis.ClientConfig{
		Addr:      args.RedisServer,
		TLSConfig: &tls.Config{},
	}
	redisClient, err := redisConfig.Materialize()
	if err != nil {
		return plane, fmt.Errorf("failed to create redis client: %v", err)
	}

	kafkaConsumerConfig := kafka.RemoteConsumerConfig{
		BootstrapServer: args.KafkaServer,
		Username:        args.KafkaUsername,
		Password:        args.KafkaPassword,
		// TODO: configure topic id, group id, and offset policy.
		GroupID:      "test",
		Topic:        "actions",
		OffsetPolicy: "earliest",
	}
	kafkaConsumer, err := kafkaConsumerConfig.Materialize()
	if err != nil {
		return plane, fmt.Errorf("failed to create kafka consumer: %v", err)
	}

	kafkaProducerConfig := kafka.RemoteProducerConfig{
		BootstrapServer: args.KafkaServer,
		Username:        args.KafkaUsername,
		Password:        args.KafkaPassword,
		// TODO: add topic id
		Topic:         "actions",
		RecreateTopic: false,
	}
	kafkaProducer, err := kafkaProducerConfig.Materialize()
	if err != nil {
		return plane, fmt.Errorf("failed to crate kafka producer: %v", err)
	}

	return Tier{
		DB:             sqlConn.(db.Connection),
		Redis:          redisClient.(redis.Client),
		ActionConsumer: kafkaConsumer.(kafka.RemoteConsumer),
		ActionProducer: kafkaProducer.(kafka.RemoteProducer),
		Clock:          clock.Unix{},
		// TODO: Replace with actual ids.
		CustID: ftypes.CustID(1),
		ID:     ftypes.TierID(1),
		// TODO: add client to ElasticCache-backed Redis instead of MemoryDB.
		Cache: redis.NewCache(redisClient.(redis.Client)),
	}, nil
}
