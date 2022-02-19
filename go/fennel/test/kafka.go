package test

import (
	"context"
	"fmt"
	"sync"
	"time"

	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"
	"fennel/tier"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

func createMockKafka(tierID ftypes.TierID) (map[string]fkafka.FProducer, tier.KafkaConsumerCreator, error) {
	brokerMap := make(map[string]*localBroker)
	producers := make(map[string]fkafka.FProducer)
	for _, topic := range fkafka.ALL_TOPICS {
		broker := newLocalBroker()
		brokerMap[topic] = &broker
		prodConfig := localProducerConfig{
			broker: &broker,
			topic:  topic,
		}
		kProducer, err := prodConfig.Materialize(tierID)
		if err != nil {
			return nil, nil, err
		}
		producers[topic] = kProducer.(fkafka.FProducer)
	}
	consumerCreator := func(topic, groupID, offsetPolicy string) (fkafka.FConsumer, error) {
		broker, ok := brokerMap[topic]
		if !ok {
			return nil, fmt.Errorf("unrecognized topic: %v", topic)
		}
		kConsumer, err := localConsumerConfig{
			broker: broker,
			Topic:  topic,
		}.Materialize(tierID)
		if err != nil {
			return nil, err
		}
		return kConsumer.(fkafka.FConsumer), nil
	}
	return producers, consumerCreator, nil
}

func setupKafkaTopics(tierID ftypes.TierID, host, username, password string, topics []string) error {
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = resource.TieredName(tierID, topic)
	}
	// Create admin client
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// now create the topics
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	specs := make([]kafka.TopicSpecification, len(names))
	for i, name := range names {
		specs[i] = kafka.TopicSpecification{
			Topic:             name,
			NumPartitions:     1,
			ReplicationFactor: 0,
		}
	}
	results, err := c.CreateTopics(ctx, specs)
	if err != nil {
		return fmt.Errorf("failed to create topics: %v", err)
	}
	for _, tr := range results {
		if tr.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf(tr.Error.Error())
		}
	}
	return nil
}

func teardownKafkaTopics(tierID ftypes.TierID, host, username, password string, topics []string) error {
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = resource.TieredName(tierID, topic)
	}
	// Create admin client.
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// delete any existing topics of these names
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.DeleteTopics(ctx, names)
	return err
}

//=================================
// Local broker (for tests)
//=================================

/*
	This has no notion of partitions, as a result, there are no
	partition re-assignments and hence no notions of commits either
*/
type localBroker struct {
	id    string
	msgs  [][]byte
	nexts map[string]int
	mutex sync.Mutex
}

func newLocalBroker() localBroker {
	return localBroker{
		id:    utils.RandString(5),
		msgs:  make([][]byte, 0),
		nexts: make(map[string]int),
	}
}

func (l *localBroker) Log(msg []byte) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	//fmt.Printf("in brokern log: %s, %v, %v\n", l.id, l.msgs, l.nexts)
	l.msgs = append(l.msgs, msg)
	//fmt.Printf("in brokern log after append: %s, %v, %v\n", l.id, l.msgs, l.nexts)
}

func (l *localBroker) Read(groupID string) ([]byte, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	//fmt.Printf("in brokern read: %s, %v, %v\n", l.id, l.msgs, l.nexts)
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

func (l *localBroker) Backlog(groupID string) int {
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

type localConsumer struct {
	tierID  ftypes.TierID
	groupid string
	Topic   string
	broker  *localBroker
}

func (l localConsumer) ReadProto(message proto.Message, timeout time.Duration) error {
	ser, err := l.broker.Read(l.groupid)
	if err != nil {
		return err
	}
	return proto.Unmarshal(ser, message)
}

func (l localConsumer) ReadBatch(upto int, timeout time.Duration) ([][]byte, error) {
	ret := make([][]byte, 0)
	for len(ret) < upto {
		msg, err := l.broker.Read(l.groupid)
		if err != nil {
			return nil, err
		}
		ret = append(ret, msg)
	}
	return ret, nil
}

func (l localConsumer) Commit() error {
	return nil
}

func (l localConsumer) AsyncCommit() chan error {
	ret := make(chan error)
	ret <- nil
	close(ret)
	return ret
}

func (l localConsumer) GroupID() string {
	return l.groupid
}

func (l localConsumer) TierID() ftypes.TierID {
	return l.tierID
}

func (l localConsumer) Close() error {
	return nil
}

func (l localConsumer) Type() resource.Type {
	return resource.KafkaConsumer
}

func (l localConsumer) Backlog() (int, error) {
	return l.broker.Backlog(l.groupid), nil
}

var _ fkafka.FConsumer = localConsumer{}

//=================================
// Config for localConsumer
//=================================

type localConsumerConfig struct {
	broker  *localBroker
	Topic   string
	groupid string
}

func (l localConsumerConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	return localConsumer{tierID, l.groupid, l.Topic, l.broker}, nil
}

var _ resource.Config = localConsumerConfig{}

//=================================
// Local producer(for tests)
//=================================

type localProducer struct {
	tierID ftypes.TierID
	topic  string
	broker *localBroker
}

func (l localProducer) LogProto(message proto.Message, partitionKey []byte) error {
	ser, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	l.broker.Log(ser)
	return nil
}

func (l localProducer) Log(message []byte, partitionKey []byte) error {
	l.broker.Log(message)
	return nil
}

func (l localProducer) Flush(timeout time.Duration) error {
	return nil
}

func (l localProducer) TierID() ftypes.TierID {
	return l.tierID
}

func (l localProducer) Close() error {
	return nil
}

func (l localProducer) Type() resource.Type {
	return resource.KafkaProducer
}

var _ fkafka.FProducer = localProducer{}

//=================================
// Config for localProducer
//=================================

type localProducerConfig struct {
	broker *localBroker
	topic  string
}

func (conf localProducerConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	return localProducer{tierID, conf.topic, conf.broker}, nil
}

var _ resource.Config = localProducerConfig{}
