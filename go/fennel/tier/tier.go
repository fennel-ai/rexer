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

type TierArgs struct {
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
	ID        ftypes.TierID
	CustID    ftypes.CustID
	DB        db.Connection
	Redis     redis.Client
	Cache     cache.Cache
	Producers map[string]kafka.FProducer
	Consumers map[string]kafka.FConsumer
	Clock     clock.Clock
}

func CreateFromArgs(args *TierArgs) (tier Tier, err error) {
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
		return tier, fmt.Errorf("failed to connect with mysql: %v", err)
	}

	redisConfig := redis.ClientConfig{
		Addr:      args.RedisServer,
		TLSConfig: &tls.Config{},
	}
	redisClient, err := redisConfig.Materialize()
	if err != nil {
		return tier, fmt.Errorf("failed to create redis client: %v", err)
	}
	producers, consumers, err := CreateKafka(tierID, args.KafkaServer, args.KafkaUsername, args.KafkaPassword)
	if err != nil {
		return tier, err
	}

	return Tier{
		DB:        sqlConn.(db.Connection),
		Redis:     redisClient.(redis.Client),
		Producers: producers,
		Consumers: consumers,
		Clock:     clock.Unix{},
		// TODO: Replace with actual ids.
		CustID: ftypes.CustID(1),
		ID:     ftypes.TierID(1),
		// TODO: add client to ElasticCache-backed Redis instead of MemoryDB.
		Cache: redis.NewCache(redisClient.(redis.Client)),
	}, nil
}

func CreateKafka(tierID ftypes.TierID, server, username, password string) (map[string]kafka.FProducer, map[string]kafka.FConsumer, error) {
	producers := make(map[string]kafka.FProducer)
	consumers := make(map[string]kafka.FConsumer)
	for _, topic := range kafka.ALL_TOPICS {
		kafkaProducerConfig := kafka.RemoteProducerConfig{
			TierID:          tierID,
			BootstrapServer: server,
			Username:        username,
			Password:        password,
			Topic:           topic,
		}
		kafkaProducer, err := kafkaProducerConfig.Materialize()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to crate kafka producer: %v", err)
		}
		producers[topic] = kafkaProducer.(kafka.FProducer)

		kafkaConsumerConfig := kafka.RemoteConsumerConfig{
			TierID:          tierID,
			BootstrapServer: server,
			Username:        username,
			Password:        password,
			// TODO: configure group id, and offset policy.
			GroupID:      "test",
			Topic:        topic,
			OffsetPolicy: "earliest",
		}
		kafkaConsumer, err := kafkaConsumerConfig.Materialize()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		consumers[topic] = kafkaConsumer.(kafka.FConsumer)
	}
	return producers, consumers, nil
}
