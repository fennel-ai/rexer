package kafka

import (
	"context"
	"fennel/airbyte"
	"fennel/lib/nitrous"
	"time"

	"fennel/lib/action"
	"fennel/lib/counter"
	"fennel/lib/feature"
	"fennel/lib/profile"
	"fennel/lib/usage"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

const (
	SecurityProtocol         = "SASL_SSL"
	SaslPlainMechanism       = "PLAIN"
	SaslScramSha512Mechanism = "SCRAM-SHA-512"
	DefaultOffsetPolicy      = "earliest"
	EarliestOffsetPolicy     = "earliest"
	LatestOffsetPolicy       = "latest"
)

type FConsumer interface {
	resource.Resource
	Name() string
	Read(ctx context.Context, timeout time.Duration) ([]byte, error)
	ReadProto(ctx context.Context, message proto.Message, timeout time.Duration) error
	ReadBatch(ctx context.Context, upto int, timeout time.Duration) ([][]byte, error)
	Backlog() (int, error)
	Commit() (kafka.TopicPartitions, error)
	CommitOffsets(kafka.TopicPartitions) (kafka.TopicPartitions, error)
	Offsets() (kafka.TopicPartitions, error)
	GetPartitions() (kafka.TopicPartitions, error)
	GroupID() string
}

type FProducer interface {
	resource.Resource
	LogProtoToPartition(ctx context.Context, message proto.Message, partition int32, partitionKey []byte) error
	LogProto(ctx context.Context, message proto.Message, partitionKey []byte) error
	LogToPartition(ctx context.Context, message []byte, partition int32, partitionKey []byte) error
	Log(ctx context.Context, message []byte, partitionKey []byte) error
	Flush(timeout time.Duration) error
}

type TopicConf struct {
	Scope    resource.Scope
	Topic    string
	PConfigs ProducerConfigs
	CConfigs ConsumerConfigs
}

var ALL_TOPICS = []TopicConf{
	{Scope: resource.TierScope{}, Topic: action.ACTIONLOG_KAFKA_TOPIC},
	// TODO: Deprecate `ACTIONLOG_JSON_KAFKA_TOPIC` once confluent go supports
	// producing and consuming schema versioned messages
	{Scope: resource.TierScope{}, Topic: action.ACTIONLOG_JSON_KAFKA_TOPIC},

	// NOTE: features kafka topic has multiple partitions.
	{
		Scope: resource.TierScope{},
		Topic: feature.KAFKA_TOPIC_NAME,
		PConfigs: ProducerConfigs{
			// controls how many records are batched together and sent as a single request to the broker (one for each partition)
			// size in bytes; default=16384
			"batch.size=327680",
			// upper bound on the delay for batching of records
			// if the local queue has records of size `batch.size`, this delay is respected (sent ASAP), but in the absence
			// of load, this is the artificial delay introduced before sending batch of records; default=0 (sent immediately)
			"linger.ms=10",
			// This enabled compression for the batch of records sent to the broker.
			// See - https://coda.io/d/Kafka-compression-choice_dbRQr4J-BJo/Kafka-compression-choice_suZIg#_lu2wy
			//
			// For configurations - https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#topic-configuration-properties
			//
			// NOTE: This will require consumers of this topic to have the same compression type set to decompress the
			// batch of records consumed by it
			"compression.type=zstd",
			"compression.level=1",
		},
	},
	{Scope: resource.TierScope{}, Topic: profile.PROFILELOG_KAFKA_TOPIC},
	{Scope: resource.TierScope{}, Topic: counter.AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME},
	{Scope: resource.TierScope{}, Topic: usage.HOURLY_USAGE_LOG_KAFKA_TOPIC},
	{Scope: resource.TierScope{}, Topic: airbyte.AIRBYTE_KAFKA_TOPIC},
	{
		Scope: resource.PlaneScope{},
		Topic: nitrous.BINLOG_KAFKA_TOPIC,
		PConfigs: ProducerConfigs{
			// We notice that a lot of messages are queued before being flushed to the broker. To increase
			// producer throughput, we tune the following configurations -
			//
			// controls how many records are batched together and sent as a single request to the broker (one for each partition)
			// size in bytes; default=16384
			"batch.size=327680",
			// upper bound on the delay for batching of records
			// if the local queue has records of size `batch.size`, this delay is respected (sent ASAP), but in the absence
			// of load, this is the artificial delay introduced before sending batch of records; default=0 (sent immediately)
			"linger.ms=10",
		},
		CConfigs: ConsumerConfigs{
			// `max.partition.fetch.bytes` dictates the initial maximum number of bytes requested per
			// broker+partition.
			//
			// this could be restricted by `max.message.bytes` (topic) or `message.max.bytes` (broker) config
			"max.partition.fetch.bytes=2097164",
			// Maximum amount of data the broker shall return for a Fetch request.
			// Since this topic has consumers = partitions, this should preferably be
			// `max.partition.fetch.bytes x #partitions`
			"fetch.max.bytes=67109248",
		},
	},
	{
		Scope: resource.PlaneScope{},
		Topic: nitrous.REQS_KAFKA_TOPIC,
		PConfigs: ProducerConfigs{
			// We notice that a lot of messages are queued before being flushed to the broker. To increase
			// producer throughput, we tune the following configurations -
			//
			// controls how many records are batched together and sent as a single request to the broker (one for each partition)
			// size in bytes; default=16384
			"batch.size=327680",
			// upper bound on the delay for batching of records
			// if the local queue has records of size `batch.size`, this delay is respected (sent ASAP), but in the absence
			// of load, this is the artificial delay introduced before sending batch of records; default=0 (sent immediately)
			"linger.ms=10",
		},
		CConfigs: ConsumerConfigs{
			// `max.partition.fetch.bytes` dictates the initial maximum number of bytes requested per
			// broker+partition.
			//
			// this could be restricted by `max.message.bytes` (topic) or `message.max.bytes` (broker) config
			"max.partition.fetch.bytes=2097164",
			// Maximum amount of data the broker shall return for a Fetch request.
			// Since this topic has consumers = partitions, this should preferably be
			// `max.partition.fetch.bytes x #partitions`
			"fetch.max.bytes=67109248",
		},
	},
	{Scope: resource.PlaneScope{}, Topic: nitrous.AGGR_CONF_KAFKA_TOPIC},
}

func ConfigMap(server, username, password, saslMechanism string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": server,
		"sasl.username":     username,
		"sasl.password":     password,
		"security.protocol": SecurityProtocol,
		"sasl.mechanisms":   saslMechanism,
		// gather statistics every 1s
		"statistics.interval.ms": 1 * 1000,
		// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
		// default: 100000
		"queue.buffering.max.messages": 2000000,
		// default: 1048576
		// this has a higher priority than `queue.buffering.max.messages`
		"queue.buffering.max.kbytes": 2097152,
	}
}
