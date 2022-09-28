package server

import (
	"context"
	"errors"
	"fennel/lib/arena"
	"fmt"
	"path"
	"sync"
	"time"

	"fennel/gravel"
	"fennel/hangar"
	"fennel/hangar/encoders"
	gravelDB "fennel/hangar/gravel"
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
			lag, err := ndb.GetLag()
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
	nos             nitrous.Nitrous
	aggregateTailer *tailer.Tailer
	binlogTailers   []*tailer.Tailer
	// sync map to avoid concurrent access in errgroup - this is usually flagged by go test -race
	shards           *sync.Map
	tables           *sync.Map
	binlogPartitions uint32
}

func getPartitions(n nitrous.Nitrous, topic string) (kafka.TopicPartitions, error) {
	// Create a temporary consumer to read topic metadata.
	consumer, err := n.KafkaConsumerFactory(fkafka.ConsumerConfig{
		Scope:        resource.NewPlaneScope(n.PlaneID),
		Topic:        topic,
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
		nos:              n,
		tables:           new(sync.Map),
		shards:           new(sync.Map),
		binlogPartitions: n.BinlogPartitions,
	}
	// Initialize a binlog tailer per topic partition.
	tailers := make([]*tailer.Tailer, 0, len(n.Partitions))
	toppars, err := getPartitions(n, libnitrous.BINLOG_KAFKA_TOPIC)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic partitions: %w", err)
	}

	// if the assigned partitions are empty, assume all binlog partitions
	//
	// else, filter out the topic partitions which are assigned to this nitrous instance
	requiredToppar := make(kafka.TopicPartitions, 0, len(n.Partitions))
	if len(n.Partitions) == 0 {
		requiredToppar = toppars
	} else {
		for _, par := range n.Partitions {
			for _, toppar := range toppars {
				if toppar.Partition == par {
					requiredToppar = append(requiredToppar, toppar)
				}
			}
		}
	}

	for _, toppar := range requiredToppar {
		// Instantiate gravel instance per tailer
		//
		// We set the `MaxTableSize` for each gravel instance taking total system memory into consideration.
		// We expect the following entities to be in-memory:
		// i) Memtable of each tailer
		// ii) Index of the files in the disk (using mmap) for fast lookups
		// iii) >= 2 files loaded into memory for compaction
		//
		// + leaving some room for any unexpected entities around
		//
		// The value here is selected taking into consideration that Nitrous could run on a machine with <= 100GB of
		// memory to be cost efficient
		gravelOpts := gravel.DefaultOptions().WithMaxTableSize(128 << 20).WithName(fmt.Sprintf("binlog-%d", toppar.Partition)).WithNumShards(16).WithCompactionWorkerNum(2)
		gravelDb, err := gravelDB.NewHangar(n.PlaneID, path.Join(n.DbDir, fmt.Sprintf("%d", toppar.Partition)), &gravelOpts, encoders.Default())
		if err != nil {
			return nil, err
		}
		t, err := tailer.NewTailer(n, libnitrous.BINLOG_KAFKA_TOPIC, toppar, gravelDb, ndb.Process, tailer.DefaultPollTimeout, tailer.DefaultTailerBatch)
		if err != nil {
			return nil, fmt.Errorf("failed to setup tailer for partition %v: %w", toppar.Partition, err)
		}
		tailers = append(tailers, t)
		ndb.shards.Store(toppar.Partition, gravelDb)
	}
	ndb.binlogTailers = tailers

	// Create gravel for aggregate definitions, we don't expect a lot of data to be here, so we use a small ~10MB
	// memtable
	aggOpts := gravel.DefaultOptions().WithMaxTableSize(10 << 20).WithName("aggdef").WithCompactionWorkerNum(1) // 10 MB
	aggregatesDb, err := gravelDB.NewHangar(n.PlaneID, path.Join(n.DbDir, "aggdef"), &aggOpts, encoders.Default())
	if err != nil {
		return nil, err
	}

	// Be aggressive on the poll timeout for aggregate configurations as we want to apply any of the aggregate
	// configuration changes before potentially any of the messages in binlog correspond to the aggregate
	//
	// NOTE: There is still a possibility that few of the earlier messages consumed in the binlog may not find
	// the corresponding aggregate. Given the nature of the traffic (only on defined aggregates, binlog can be created
	// with the stream of actions fetched from kafka and later produced to binlog kafka topic) and the below
	// configuration, this should almost never happen
	//
	// Also we don't expect to receive a lot of aggregate configuration updates (except potentially on clean startup)
	// so keep the batch size low
	aggrConfToppars, err := getPartitions(n, libnitrous.AGGR_CONF_KAFKA_TOPIC)
	if err != nil {
		return nil, err
	}
	if len(aggrConfToppars) > 1 {
		return nil, fmt.Errorf("expected aggregate conf topic partitions to be 1, found: %d", len(aggrConfToppars))
	}
	ndb.aggregateTailer, err = tailer.NewTailer(n, libnitrous.AGGR_CONF_KAFKA_TOPIC, aggrConfToppars[0], aggregatesDb, ndb.ProcessAggregates, 1*time.Second /*pollTimeout*/, 100 /*batchSize*/)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregate conf tailer: %v", err)
	}
	ndb.startReportingKafkaLag()

	// Restore aggregate definitions.
	err = ndb.restoreAggregates(aggregatesDb)
	if err != nil {
		return nil, fmt.Errorf("failed to restore aggregate definitions: %w", err)
	}
	return ndb, nil
}

func (ndb *NitrousDB) Start() {
	// Start tailing aggregate configuration tailer before tailing the binlog. As noted above, it is highly likely
	// that during regular traffic pattern, we will run into a situation where binlog tailers consume messages
	// corresponding to an aggregate which was not consumed by the tailer by then.
	//
	// However, on nitrous startup, where it might restore a snapshot at certain aggregate conf and binlog offsets,
	// if all the tailers start tailing at the same time, binlog messages corresponding to an aggregate not defined then
	// could be consumed and discarded. To avoid this scenario, we start tailing the aggregate configuration
	// first, wait for it to finish before tailing binlog.
	//
	// In the scenario where aggregate configurations might have a "CREATE" and "DELETE" event for an aggregate,
	// say they were in reality separated by few hours/day, we ingest them together and resulting in no-op in the
	// aggregate table, however there could be binlog messages for this aggregate. They will simply be discarded.
	// This is fine, as we provide "live-ness" guarantees only for the "ACTIVE" aggregates.
	go ndb.aggregateTailer.Tail()
	count := 0
	for count < 3 {
		lag, err := ndb.aggregateTailer.GetLag()
		if err != nil {
			zap.L().Error("failed to get lag for aggregate configuration", zap.Error(err))
		}
		// consumed all the messages
		if lag == 0 {
			// just to be sure, wait for the tailer to report zero lag for few attempts
			count++
		}
		// sleep for the PollTimeout since at least after this time, a new batch of data will be read by the tailer
		time.Sleep(ndb.aggregateTailer.GetPollTimeout())
	}
	for _, t := range ndb.binlogTailers {
		go t.Tail()
	}
}

func (ndb *NitrousDB) Stop() {
	// Stop tailing for new updates.
	for _, t := range ndb.binlogTailers {
		t.Stop()
	}

	// Stop the aggregate tailer later - if this was stopped earlier, it is possible that we did not consume
	// an aggregate configuration but kept consuming binlog with messages corresponding to this aggregate
	ndb.aggregateTailer.Stop()
	// TODO: Should we close the store as well?
	// _ = ndb.nos.Store.Close()
}

func (ndb *NitrousDB) SetAggrConfPollTimeout(d time.Duration) {
	ndb.aggregateTailer.SetPollTimeout(d)
}

func (ndb *NitrousDB) GetAggrConfPollTimeout() time.Duration {
	return ndb.aggregateTailer.GetPollTimeout()
}

func (ndb *NitrousDB) SetBinlogPollTimeout(d time.Duration) {
	for _, t := range ndb.binlogTailers {
		t.SetPollTimeout(d)
	}
}

func (ndb *NitrousDB) GetBinlogPollTimeout() time.Duration {
	if len(ndb.binlogTailers) == 0 {
		return 0
	}
	return ndb.binlogTailers[0].GetPollTimeout()
}

func (ndb *NitrousDB) ProcessAggregates(_ context.Context, ops []*rpc.NitrousOp, _ hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	var delta hangar.ValGroup
	for _, op := range ops {
		switch op.Type {
		case rpc.OpType_CREATE_AGGREGATE:
			event := op.GetCreateAggregate()
			d, err := ndb.processCreateEvent(ftypes.RealmID(op.TierId), event)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to initialize aggregate: %w", err)
			}
			err = delta.Update(d)
			if err != nil {
				return nil, nil, err
			}
		case rpc.OpType_DELETE_AGGREGATE:
			event := op.GetDeleteAggregate()
			tierId := ftypes.RealmID(op.TierId)
			d, err := ndb.processDeleteEvent(tierId, event)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to deactivate aggregate: %w", err)
			}
			err = delta.Update(d)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return []hangar.Key{{Data: agg_table_key}}, []hangar.ValGroup{delta}, nil
}

func (ndb *NitrousDB) Process(ctx context.Context, ops []*rpc.NitrousOp, reader hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
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
			return fmt.Errorf("faild to unmarshal store proto options for aggId %d in tier %d: %w", aggId, tierId, err)
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

func (ndb *NitrousDB) Get(ctx context.Context, tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, groupkeys []string, kwargs []value.Dict, ret []value.Value) error {
	// Get the aggregate store for this aggregate.
	v, ok := ndb.tables.Load(aggKey{tierId, aggId, codec})
	if !ok {
		return fmt.Errorf("no table for aggregate %d in tier %d with codec %d", aggId, tierId, codec)
	}
	// figure out the shards where the groups keys will be situated
	shardToGkIdx := make(map[int32][]int, 0)
	for i, gk := range groupkeys {
		// get shard
		shard := int32(nitrous.HashedPartition(gk, ndb.binlogPartitions))
		if _, ok := shardToGkIdx[shard]; !ok {
			shardToGkIdx[shard] = make([]int, 0)
		}
		shardToGkIdx[shard] = append(shardToGkIdx[shard], i)
	}
	egrp, _ := errgroup.WithContext(ctx)
	for s, is := range shardToGkIdx {
		shard := s
		indices := is
		egrp.Go(func() error {
			s, ok := ndb.shards.Load(shard)
			if !ok {
				return fmt.Errorf("failed to load gravel instance for shard: %d", shard)
			}
			if s, ok = s.(hangar.Hangar); !ok {
				return fmt.Errorf("instance loaded from the shards is not a hangar instance: %v", s)
			}

			keys := arena.Strings.Alloc(len(indices), len(indices))
			defer arena.Strings.Free(keys)
			args := arena.DictValues.Alloc(len(indices), len(indices))
			defer arena.DictValues.Free(args)
			// TODO(mohit): This seems inefficient.. ret is already allocated from Arena and then we are
			// allocating parts of the ret through Arena again
			//
			// consider passing id mapping to write to the correct indices directly in `ret`
			vals := arena.Values.Alloc(len(indices), len(indices))
			defer arena.Values.Free(vals)
			for i, id := range indices {
				keys[i] = groupkeys[id]
				args[i] = kwargs[id]
			}
			err := v.(store.Table).Get(ctx, keys, args, s.(hangar.Hangar), vals)
			if err != nil {
				return err
			}
			if len(vals) != len(keys) {
				return fmt.Errorf("did not get expected vals: %v v/s %v, part: %v", len(vals), len(keys), shard)
			}
			for i, id := range indices {
				ret[id] = vals[i]
			}
			return nil
		})
	}
	if err := egrp.Wait(); err != nil {
		return fmt.Errorf("failed to get values from sharded gravel: %v", err)
	}
	return nil
}

func (ndb *NitrousDB) GetLag() (int, error) {
	// TODO(mohit): Consider reporting aggregate conf lag separately
	lag := 0
	l, err := ndb.aggregateTailer.GetLag()
	if err != nil {
		return lag, fmt.Errorf("failed to get lag for the aggregate configuration tailer: %v", err)
	}
	lag += l
	for _, t := range ndb.binlogTailers {
		l, err := t.GetLag()
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
