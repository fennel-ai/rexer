package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"

	"fennel/lib/utils"
	"fennel/lib/utils/ptr"
	"fennel/resource"
)

/*
	This file defines mock producer/consumers relying on a mock in-memory broker
	that has no notion of partitions. As a result, there are no partition re-assignments
	and hence no notions of commits either.
*/

type MockBroker struct {
	id      string
	msgs    [][]byte
	nexts   map[string]int
	commits map[string]int
	mutex   sync.Mutex
}

func NewMockTopicBroker() MockBroker {
	return MockBroker{
		id:      utils.RandString(5),
		msgs:    make([][]byte, 0),
		nexts:   make(map[string]int),
		commits: make(map[string]int),
	}
}

func (l *MockBroker) InitConsumer(groupID string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.nexts[groupID] = 0
}

func (l *MockBroker) Log(msg []byte) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.msgs = append(l.msgs, msg)
}

func (l *MockBroker) Read(groupID string) ([]byte, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	nxt := l.nexts[groupID]
	if nxt >= len(l.msgs) {
		return nil, fmt.Errorf("no new messages")
	}
	l.nexts[groupID] = nxt + 1
	return l.msgs[nxt], nil
}

func (l *MockBroker) Backlog(groupID string) int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if offset, ok := l.commits[groupID]; !ok {
		return len(l.msgs)
	} else {
		return (len(l.msgs) - 1) - offset
	}
}

func (l *MockBroker) Commit(groupID string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.commits[groupID] = l.nexts[groupID] - 1
}

func (l *MockBroker) Offset(groupID string) int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	for k, v := range l.nexts {
		if groupID == k {
			return v
		}
	}
	return 0
}

//=================================
// Local consumer(for tests)
//=================================

type mockConsumer struct {
	resource.Scope
	groupid string
	Topic   string
	broker  *MockBroker
}

func (l mockConsumer) Name() string {
	return "consumer"
}

func (l mockConsumer) Read(ctx context.Context, timeout time.Duration) ([]byte, error) {
	ticker := time.Tick(timeout) //nolint
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled - no new messages to read")
		case <-ticker:
			return nil, fmt.Errorf("timeout - no new messages to read")
		default:
			ser, err := l.broker.Read(l.groupid)
			if err == nil {
				return ser, nil
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (l mockConsumer) ReadProto(ctx context.Context, message proto.Message, timeout time.Duration) error {
	ticker := time.Tick(timeout) //nolint
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled - no new messages to read")
		case <-ticker:
			return fmt.Errorf("timeout - no new messages to read")
		default:
			ser, err := l.broker.Read(l.groupid)
			if err == nil {
				return proto.Unmarshal(ser, message)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (l mockConsumer) ReadBatch(ctx context.Context, upto int, timeout time.Duration) ([][]byte, error) {
	ret := make([][]byte, 0)
	ticker := time.Tick(timeout) //nolint
	for len(ret) < upto {
		select {
		case <-ctx.Done():
			return ret, nil
		case <-ticker:
			return ret, nil
		default:
			msg, err := l.broker.Read(l.groupid)
			if err == nil {
				ret = append(ret, msg)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	return ret, nil
}

func (l mockConsumer) Commit() (kafka.TopicPartitions, error) {
	l.broker.Commit(l.groupid)
	return nil, nil
}

func (l mockConsumer) CommitOffsets(kafka.TopicPartitions) (kafka.TopicPartitions, error) {
	l.broker.Commit(l.groupid)
	return nil, nil
}

func (l mockConsumer) Offsets() (kafka.TopicPartitions, error) {
	offset := l.broker.Offset(l.groupid)
	return kafka.TopicPartitions{
		{Topic: &l.Topic, Partition: 0, Offset: kafka.Offset(offset)},
	}, nil
}

func (l mockConsumer) GroupID() string {
	return l.groupid
}

func (l mockConsumer) Close() error {
	return nil
}

func (l mockConsumer) Type() resource.Type {
	return resource.KafkaConsumer
}

func (l mockConsumer) Backlog() (int, error) {
	return l.broker.Backlog(l.groupid), nil
}

func (l mockConsumer) GetPartitions() (kafka.TopicPartitions, error) {
	return kafka.TopicPartitions{{Topic: ptr.To(l.Topic), Partition: 0}}, nil
}

var _ FConsumer = mockConsumer{}

//=================================
// Config for localConsumer
//=================================

type MockConsumerConfig struct {
	Broker  *MockBroker
	Topic   string
	GroupID string
	Scope   resource.Scope
}

func (l MockConsumerConfig) Materialize() (resource.Resource, error) {
	topic := l.Scope.PrefixedName(l.Topic)
	l.Broker.InitConsumer(l.GroupID)
	return mockConsumer{l.Scope, l.GroupID, topic, l.Broker}, nil
}

var _ resource.Config = MockConsumerConfig{}

//=================================
// Local producer(for tests)
//=================================

type mockProducer struct {
	resource.Scope
	topic  string
	broker *MockBroker
}

func (l mockProducer) LogProtoToPartition(ctx context.Context, message proto.Message, partition int32, partitionKey []byte) error {
	return l.LogProto(ctx, message, partitionKey)
}

func (l mockProducer) LogToPartition(ctx context.Context, message []byte, partition int32, partitionKey []byte) error {
	return l.Log(ctx, message, partitionKey)
}

func (l mockProducer) LogProto(_ context.Context, message proto.Message, partitionKey []byte) error {
	ser, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	l.broker.Log(ser)
	return nil
}

func (l mockProducer) Log(ctx context.Context, message []byte, partitionKey []byte) error {
	l.broker.Log(message)
	return nil
}

func (l mockProducer) Flush(timeout time.Duration) error {
	return nil
}

func (l mockProducer) Close() error {
	return nil
}

func (l mockProducer) Type() resource.Type {
	return resource.KafkaProducer
}

var _ FProducer = mockProducer{}

//=================================
// Config for localProducer
//=================================

type MockProducerConfig struct {
	Broker *MockBroker
	Topic  string
	Scope  resource.Scope
}

func (conf MockProducerConfig) Materialize() (resource.Resource, error) {
	topic := conf.Scope.PrefixedName(conf.Topic)
	return mockProducer{conf.Scope, topic, conf.Broker}, nil
}

var _ resource.Config = MockProducerConfig{}
