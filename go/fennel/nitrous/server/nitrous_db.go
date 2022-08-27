package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"fennel/hangar"
	fkafka "fennel/kafka"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	libnitrous "fennel/lib/nitrous"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server/store"
	"fennel/nitrous/server/tailer"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

const (
	MAX_TAILERS = 8
)

var (
	agg_table_key = []byte("agg_table")

	backlog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nitrous_tailer_backlog",
		Help: "Backlog of tailer",
	})
)

// Start a go-routine that periodically reports the tailer's backlog to prometheus.
func (ndb *NitrousDB) startReportingKafkaLag() {
	go func() {
		for range time.Tick(10 * time.Second) {
			lag, err := ndb.GetLag(context.Background())
			if err != nil {
				zap.L().Error("Failed to get kafka backlog", zap.Error(err))
			}
			backlog.Set(float64(lag))
		}
	}()
}

type aggKey struct {
	tierId ftypes.RealmID
	aggId  ftypes.AggId
	codec  rpc.AggCodec
}

type NitrousDB struct {
	nos     nitrous.Nitrous
	tailers []*tailer.Tailer
	tables  *sync.Map
}

func getPartitions(n nitrous.Nitrous) (kafka.TopicPartitions, error) {
	// Create a temporary consumer to read topic metadata.
	consumer, err := n.KafkaConsumerFactory(fkafka.ConsumerConfig{
		Scope:        resource.NewPlaneScope(n.PlaneID),
		Topic:        libnitrous.BINLOG_KAFKA_TOPIC,
		GroupID:      "metadata_consumer",
		OffsetPolicy: fkafka.LatestOffsetPolicy,
		// here it does not matter which broker this consumer connects to, since the information read is quite
		// low
		Configs: fkafka.ConsumerConfigs{
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
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}
	defer consumer.Close()
	return consumer.GetPartitions()
}

func InitDB(n nitrous.Nitrous) (*NitrousDB, error) {
	ndb := &NitrousDB{
		nos:    n,
		tables: new(sync.Map),
	}
	// Initialize a binlog tailer per topic partition.
	toppars, err := getPartitions(n)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic partitions: %w", err)
	}
	numTailers := MAX_TAILERS
	if len(toppars) < numTailers {
		numTailers = len(toppars)
	}
	numPartitionsPerTailer := (len(toppars) + numTailers - 1) / numTailers
	tailers := make([]*tailer.Tailer, 0, numTailers)
	for start := 0; start < len(toppars); start += numPartitionsPerTailer {
		end := start + numPartitionsPerTailer
		if end > len(toppars) {
			end = len(toppars)
		}
		partitions := toppars[start:end]
		tailer, err := tailer.NewTailer(n, libnitrous.BINLOG_KAFKA_TOPIC, partitions, ndb.Process)
		if err != nil {
			return nil, fmt.Errorf("failed to setup tailer for partition(s) %v: %w", partitions, err)
		}
		tailers = append(tailers, tailer)
	}
	ndb.tailers = tailers
	ndb.startReportingKafkaLag()
	// Restore aggregate definitions.
	err = ndb.restoreAggregates(n.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to restore aggregate definitions: %w", err)
	}
	return ndb, nil
}

func (ndb *NitrousDB) Start() {
	for _, tailer := range ndb.tailers {
		go tailer.Tail()
	}
}

func (ndb *NitrousDB) Stop() {
	// Stop tailing for new updates.
	for _, tailer := range ndb.tailers {
		tailer.Stop()
	}
	// TODO: Should we close the store as well?
	// _ = ndb.nos.Store.Close()
}

func (ndb *NitrousDB) SetPollTimeout(d time.Duration) {
	for _, tailer := range ndb.tailers {
		tailer.SetPollTimeout(d)
	}
}

func (ndb *NitrousDB) GetPollTimeout() time.Duration {
	if len(ndb.tailers) == 0 {
		return 0
	}
	return ndb.tailers[0].GetPollTimeout()
}

func (ndb *NitrousDB) Process(ctx context.Context, ops []*rpc.NitrousOp, reader hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	var keys []hangar.Key
	var vgs []hangar.ValGroup
	// First, process any changes to aggregate definitions.
	var tablesDelta hangar.ValGroup
	for _, op := range ops {
		switch op.Type {
		case rpc.OpType_CREATE_AGGREGATE:
			event := op.GetCreateAggregate()
			d, err := ndb.processCreateEvent(ftypes.RealmID(op.TierId), event)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to initialize aggregate: %w", err)
			}
			err = tablesDelta.Update(d)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to update valgroup: %w", err)
			}
		case rpc.OpType_DELETE_AGGREGATE:
			event := op.GetDeleteAggregate()
			tierId := ftypes.RealmID(op.TierId)
			d, err := ndb.processDeleteEvent(tierId, event)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to deactivate aggregate: %w", err)
			}
			err = tablesDelta.Update(d)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to update valgroup: %w", err)
			}
		}
	}
	if len(tablesDelta.Fields) > 0 {
		zap.L().Info("Received aggregate definition updates", zap.Int("count", len(tablesDelta.Fields)))
		keys = append(keys, hangar.Key{Data: agg_table_key})
		vgs = append(vgs, tablesDelta)
	}
	// Only then process any updates to the aggregate tables.
	tks, tvgs, err := ndb.getRowUpdates(ctx, ops, reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get row updates: %w", err)
	}
	keys = append(keys, tks...)
	vgs = append(vgs, tvgs...)
	return keys, vgs, nil
}

func (ndb *NitrousDB) getRowUpdates(ctx context.Context, ops []*rpc.NitrousOp, reader hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	var keys []hangar.Key
	var vgs []hangar.ValGroup
	type update struct {
		keys []hangar.Key
		vgs  []hangar.ValGroup
	}
	updates := make(chan update)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		for update := range updates {
			keys = append(keys, update.keys...)
			vgs = append(vgs, update.vgs...)
		}
	}()
	eg := &errgroup.Group{}
	ndb.tables.Range(func(key, value interface{}) bool {
		aggKey, table := key.(aggKey), value.(store.Table)
		tierId, aggId, codec := aggKey.tierId, aggKey.aggId, aggKey.codec
		zap.L().Debug("Iterating over new ops for table",
			zap.Uint64("tierId", uint64(tierId)), zap.Uint64("aggId", uint64(aggId)), zap.Int("codec", int(codec)))
		eg.Go(func() error {
			ks, vs, err := table.Process(ctx, ops, reader)
			if err != nil {
				zap.L().Error("Failed to process ops",
					zap.Uint64("tierId", uint64(tierId)), zap.Uint64("aggId", uint64(aggId)), zap.Int("codec", int(codec)), zap.Error(err))
				return err
			}
			updates <- update{ks, vs}
			return nil
		})
		return true
	})
	// Wait for all processors to finish and then close updates channel.
	err := eg.Wait()
	close(updates)
	<-doneCh
	if err != nil {
		return nil, nil, fmt.Errorf("One or more tables failed to process ops: %w", err)
	}
	return keys, vgs, nil
}

func (ndb *NitrousDB) restoreAggregates(h hangar.Hangar) error {
	// Get current set of aggregates for this plane.
	vgs, err := h.GetMany(context.Background(), []hangar.KeyGroup{{Prefix: hangar.Key{Data: agg_table_key}}})
	if err != nil {
		return fmt.Errorf("failed to get aggregate definitions: %w", err)
	}
	if len(vgs) == 0 {
		zap.L().Info("No aggregate definitions")
		return nil
	}
	vg := vgs[0]
	count := 0
	for i, field := range vg.Fields {
		if len(field) == 0 {
			zap.L().Warn("Skipping aggregate definition with empty field", zap.Binary("field", field), zap.Binary("value", vg.Values[i]))
			continue
		}
		_, tierId, aggId, codec, err := decodeField(field)
		if err != nil {
			return fmt.Errorf("failed to decode %s: %w", string(field), err)
		}
		if len(vg.Values[i]) == 0 {
			zap.L().Debug("Skipping deactivated aggregate", zap.Uint64("tierId", uint64(tierId)), zap.Uint64("aggId", uint64(aggId)))
			continue
		}
		var popts aggregate.AggOptions
		err = proto.Unmarshal(vg.Values[i], &popts)
		if err != nil {
			return fmt.Errorf("faield to unmarshal store proto options for aggId %d in tier %d: %w", aggId, tierId, err)
		}
		options := aggregate.FromProtoOptions(&popts)
		err = ndb.setupAggregateTable(tierId, aggId, codec, options)
		if errors.Is(err, store.ErrNotSupported) {
			zap.L().Info("Skipping codec for aggregate", zap.Uint64("tierId", uint64(tierId)), zap.Uint64("aggId", uint64(aggId)), zap.Int("codec", int(codec)))
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
		}
		count++
	}
	zap.L().Info("Restored aggregate definitions", zap.Int("active", count), zap.Int("total", len(vg.Fields)))
	return nil
}

func (ndb *NitrousDB) processDeleteEvent(tierId ftypes.RealmID, event *rpc.DeleteAggregate) (hangar.ValGroup, error) {
	aggId := ftypes.AggId(event.AggId)
	aggTables := make(map[rpc.AggCodec]store.Table)
	ndb.tables.Range(func(key, value interface{}) bool {
		k := key.(aggKey)
		if k.aggId == aggId && k.tierId == tierId {
			aggTables[k.codec] = value.(store.Table)
		}
		return true
	})
	var ret hangar.ValGroup
	for codec := range aggTables {
		ndb.tables.Delete(aggKey{tierId, aggId, codec})
		field, err := encodeField(tierId, aggId, codec)
		if err != nil {
			return hangar.ValGroup{}, fmt.Errorf("failed to encode hangar field for new aggregate %d in tier %d: %w", aggId, tierId, err)
		}
		ret.Fields = append(ret.Fields, field)
		// We use an empty value to indicate that the aggregate is deactivated.
		// We do this because we currently don't have a way to delete fields
		// when processing events from the tailer.
		ret.Values = append(ret.Values, []byte{})
	}
	return ret, nil
}

func (ndb *NitrousDB) processCreateEvent(tierId ftypes.RealmID, event *rpc.CreateAggregate) (hangar.ValGroup, error) {
	popts := event.GetOptions()
	aggId := ftypes.AggId(event.AggId)
	codecs := getCodecs(ftypes.AggType(popts.AggType))
	options := aggregate.FromProtoOptions(popts)
	fields := make(hangar.Fields, 0, len(codecs))
	values := make(hangar.Values, 0, len(codecs))
	for _, codec := range codecs {
		key := aggKey{tierId, aggId, codec}
		if v, ok := ndb.tables.Load(key); ok {
			if !v.(store.Table).Options().Equals(options) {
				return hangar.ValGroup{}, fmt.Errorf("aggregate %d in tier %d already exists with different options", aggId, tierId)
			} else {
				continue
			}
		}
		field, err := encodeField(tierId, aggId, codec)
		if err != nil {
			return hangar.ValGroup{}, fmt.Errorf("failed to encode hangar field for new aggregate %d in tier %d: %w", aggId, tierId, err)
		}
		rawopts, err := proto.Marshal(popts)
		if err != nil {
			return hangar.ValGroup{}, fmt.Errorf("failed to byte-serialize aggregate options proto: %w", err)
		}
		fields = append(fields, field)
		values = append(values, rawopts)
		err = ndb.setupAggregateTable(tierId, aggId, codec, options)
		if err != nil {
			return hangar.ValGroup{}, fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
		}
	}
	return hangar.ValGroup{Fields: fields, Values: values}, nil
}

func getCodecs(aggType ftypes.AggType) []rpc.AggCodec {
	return []rpc.AggCodec{rpc.AggCodec_V2}
}

func (ndb *NitrousDB) setupAggregateTable(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, options aggregate.Options) error {
	table, err := store.Make(tierId, aggId, codec, options, ndb.nos.Clock)
	if err != nil {
		return fmt.Errorf("failed to create aggregate store for {aggId: %d, tierId: %d, codec: %d} : %w", aggId, tierId, codec, err)
	}
	// Register table has handler for corresponding aggregate "key" defined by
	// the tier Id, aggregate Id, and codec.
	ndb.tables.Store(aggKey{tierId, aggId, codec}, table)
	return nil
}

func (ndb *NitrousDB) Get(ctx context.Context, tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, groupkeys []string, kwargs []value.Dict) ([]value.Value, error) {
	// Get the aggregate store for this aggregate.
	v, ok := ndb.tables.Load(aggKey{tierId, aggId, codec})
	if !ok {
		return nil, fmt.Errorf("no table for aggregate %d in tier %d with codec %d", aggId, tierId, codec)
	}
	return v.(store.Table).Get(ctx, groupkeys, kwargs, ndb.nos.Store)
}

func (ndb *NitrousDB) GetLag(ctx context.Context) (int, error) {
	lag := 0
	for _, tailer := range ndb.tailers {
		l, err := tailer.GetLag()
		if err != nil && !errors.Is(err, fkafka.ErrNoPartition) {
			return 0, fmt.Errorf("error getting lag: %w", err)
		}
		lag += l
	}
	return lag, nil
}

func encodeField(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec) ([]byte, error) {
	field := [30]byte{}
	curr := 0
	n, err := binary.PutUvarint(field[curr:], uint64(tierId))
	if err != nil {
		return nil, fmt.Errorf("failed to encode tier id: %w", err)
	}
	curr += n
	n, err = binary.PutUvarint(field[curr:], uint64(aggId))
	if err != nil {
		return nil, fmt.Errorf("failed to encode aggregate id: %w", err)
	}
	curr += n
	n, err = binary.PutVarint(field[curr:], int64(codec))
	if err != nil {
		return nil, fmt.Errorf("failed to encode codec id: %w", err)
	}
	curr += n
	return field[:curr], nil
}

func decodeField(buf []byte) (int, ftypes.RealmID, ftypes.AggId, rpc.AggCodec, error) {
	curr := 0
	tierId, n, err := binary.ReadUvarint(buf[curr:])
	curr += n
	if err != nil {
		return curr, 0, 0, 0, fmt.Errorf("failed to decode tier id: %w", err)
	}
	aggId, n, err := binary.ReadUvarint(buf[curr:])
	curr += n
	if err != nil {
		return curr, 0, 0, 0, fmt.Errorf("failed to decode aggregate id: %w", err)
	}
	codec, n, err := binary.ReadVarint(buf[curr:])
	curr += n
	if err != nil {
		return curr, 0, 0, 0, fmt.Errorf("failed to decode codec id: %w", err)
	}
	return curr, ftypes.RealmID(tierId), ftypes.AggId(aggId), rpc.AggCodec(codec), nil
}
