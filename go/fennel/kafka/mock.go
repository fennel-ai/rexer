package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"
)

/*
	This file defines mock producer/consumers relying on a mock in-memory broker
	that has no notion of partitions. As a result, there are no partition re-assignments
	and hence no notions of commits either.
*/

type MockBroker struct {
	id    string
	msgs  [][]byte
	nexts map[string]int
	mutex sync.Mutex
}

func NewMockTopicBroker() MockBroker {
	return MockBroker{
		id:    utils.RandString(5),
		msgs:  make([][]byte, 0),
		nexts: make(map[string]int),
	}
}

func (l *MockBroker) Log(msg []byte) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.msgs = append(l.msgs, msg)
}

func (l *MockBroker) Read(groupID string) ([]byte, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, ok := l.nexts[groupID]; !ok {
		l.nexts[groupID] = 0
	}
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
	if offset, ok := l.nexts[groupID]; !ok {
		return len(l.msgs)
	} else {
		return len(l.msgs) - offset
	}
}

//=================================
// Local consumer(for tests)
//=================================

type mockConsumer struct {
	tierID  ftypes.TierID
	groupid string
	Topic   string
	broker  *MockBroker
}

func (l mockConsumer) ReadProto(ctx context.Context, message proto.Message, timeout time.Duration) error {
	ticker := time.Tick(timeout)
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
	ticker := time.Tick(timeout)
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

func (l mockConsumer) Commit() error {
	return nil
}

func (l mockConsumer) AsyncCommit() chan error {
	ret := make(chan error)
	ret <- nil
	close(ret)
	return ret
}

func (l mockConsumer) GroupID() string {
	return l.groupid
}

func (l mockConsumer) TierID() ftypes.TierID {
	return l.tierID
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

var _ FConsumer = mockConsumer{}

//=================================
// Config for localConsumer
//=================================

type MockConsumerConfig struct {
	Broker  *MockBroker
	Topic   string
	GroupID string
}

func (l MockConsumerConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	return mockConsumer{tierID, l.GroupID, l.Topic, l.Broker}, nil
}

var _ resource.Config = MockConsumerConfig{}

//=================================
// Local producer(for tests)
//=================================

type mockProducer struct {
	tierID ftypes.TierID
	topic  string
	broker *MockBroker
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

func (l mockProducer) TierID() ftypes.TierID {
	return l.tierID
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
}

func (conf MockProducerConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	return mockProducer{tierID, conf.Topic, conf.Broker}, nil
}

var _ resource.Config = MockProducerConfig{}
