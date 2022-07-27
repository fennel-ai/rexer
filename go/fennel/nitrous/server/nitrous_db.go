package server

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"fennel/hangar"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	libnitrous "fennel/lib/nitrous"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server/offsets"
	"fennel/nitrous/server/store"
	"fennel/nitrous/server/tailer"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	agg_table_key = []byte("agg_table")
)

type aggKey struct {
	tierId ftypes.RealmID
	aggId  ftypes.AggId
	codec  rpc.AggCodec
}

type NitrousDB struct {
	nos    nitrous.Nitrous
	tailer *tailer.Tailer
	tables *sync.Map
}

func InitDB(n nitrous.Nitrous) (*NitrousDB, error) {
	// Initialize binlog tailer.
	offsetkey := []byte("default_tailer")
	vgs, err := n.Store.GetMany(context.Background(), []hangar.KeyGroup{{Prefix: hangar.Key{Data: offsetkey}}})
	if err != nil {
		return nil, fmt.Errorf("failed to get binlog offsets: %w", err)
	}
	var toppars kafka.TopicPartitions
	if len(vgs) > 0 {
		toppars, err = offsets.DecodeOffsets(vgs[0])
		if err != nil {
			n.Logger.Fatal("Failed to restore binlog offsets from hangar", zap.Error(err))
		}
	}
	tailer, err := tailer.NewTailer(n, libnitrous.BINLOG_KAFKA_TOPIC, toppars, offsetkey)
	if err != nil {
		return nil, fmt.Errorf("failed to setup tailer: %w", err)
	}

	// Restore aggregate definitions.
	ndb := &NitrousDB{
		nos:    n,
		tailer: tailer,
		tables: new(sync.Map),
	}
	err = ndb.restoreAggregates(n.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to restore aggregate definitions: %w", err)
	}

	// Subscribe to binlog events.
	tailer.Subscribe(ndb)

	return ndb, nil
}

func (ndb *NitrousDB) Start() {
	go ndb.tailer.Tail()
}

func (ndb *NitrousDB) Stop() {
	// Stop tailing for new updates.
	ndb.tailer.Stop()
	// TODO: Should we close the store as well?
	// _ = ndb.nos.Store.Close()
}

func (ndb *NitrousDB) SetPollTimeout(d time.Duration) {
	ndb.tailer.SetPollTimeout(d)
}

func (ndb *NitrousDB) GetPollTimeout() time.Duration {
	return ndb.tailer.GetPollTimeout()
}

// Tailer-specific identity of this processor.
func (ndb *NitrousDB) Identity() string {
	return "aggdefsmgr"
}

func (ndb *NitrousDB) Process(ctx context.Context, ops []*rpc.NitrousOp, store hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	// Get current set of aggregates for this plane.
	vgs, err := store.GetMany(ctx, []hangar.KeyGroup{{Prefix: hangar.Key{Data: agg_table_key}}})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get aggregate definitions: %w", err)
	}
	var vg hangar.ValGroup
	if len(vgs) > 0 {
		vg = vgs[0]
	}
	count := 0
	for _, op := range ops {
		switch op.Type {
		case rpc.OpType_CREATE_AGGREGATE:
			count++
			event := op.GetCreateAggregate()
			vg, err = ndb.processCreateEvent(ftypes.RealmID(op.TierId), event, vg)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to initialize aggregate: %w", err)
			}
		case rpc.OpType_DELETE_AGGREGATE:
			count++
			event := op.GetDeleteAggregate()
			tierId := ftypes.RealmID(op.TierId)
			vg, err = ndb.processDeleteEvent(tierId, event, vg)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to deactivate aggregate: %w", err)
			}
		}
	}
	if count > 0 {
		ndb.nos.Logger.Info("Recieved aggregate definition updates", zap.Int("count", count))
		return []hangar.Key{{Data: agg_table_key}}, []hangar.ValGroup{vg}, nil
	} else {
		return nil, nil, nil
	}
}

func (ndb *NitrousDB) restoreAggregates(store hangar.Hangar) error {
	// Get current set of aggregates for this plane.
	vgs, err := store.GetMany(context.Background(), []hangar.KeyGroup{{Prefix: hangar.Key{Data: agg_table_key}}})
	if err != nil {
		return fmt.Errorf("failed to get aggregate definitions: %w", err)
	}
	if len(vgs) == 0 {
		ndb.nos.Logger.Info("No aggregate definitions")
		return nil
	}
	vg := vgs[0]
	for i, field := range vg.Fields {
		_, tierId, aggId, codec, err := decodeField(field)
		if err != nil {
			return fmt.Errorf("failed to decode %s: %w", string(field), err)
		}
		var popts aggregate.AggOptions
		err = proto.Unmarshal(vg.Values[i], &popts)
		if err != nil {
			return fmt.Errorf("faield to unmarshal store proto options for aggId %d in tier %d: %w", aggId, tierId, err)
		}
		options := aggregate.FromProtoOptions(&popts)
		err = ndb.setupAggregateTable(tierId, aggId, codec, options)
		if err != nil {
			return fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
		}
	}
	return nil
}

func (ndb *NitrousDB) processDeleteEvent(tierId ftypes.RealmID, event *rpc.DeleteAggregate, vg hangar.ValGroup) (hangar.ValGroup, error) {
	aggId := ftypes.AggId(event.AggId)
	aggTables := make(map[rpc.AggCodec]store.Table)
	ndb.tables.Range(func(key, value interface{}) bool {
		k := key.(aggKey)
		if k.aggId == aggId && k.tierId == tierId {
			aggTables[k.codec] = value.(store.Table)
		}
		return true
	})
	for codec, table := range aggTables {
		field, err := encodeField(tierId, aggId, codec)
		if err != nil {
			return hangar.ValGroup{}, fmt.Errorf("failed to encode hangar field for new aggregate %d in tier %d: %w", aggId, tierId, err)
		}
		fields := vg.Fields[:0]
		values := vg.Values[:0]
		for i, f := range vg.Fields {
			if bytes.Equal(f, field) {
				ndb.tables.Delete(aggKey{tierId, aggId, codec})
				ndb.tailer.Unsubscribe(table.Identity())
				continue
			}
			fields = append(fields, field)
			values = append(values, vg.Values[i])
		}
		vg.Fields = fields
		vg.Values = values
	}
	return vg, nil
}

func (ndb *NitrousDB) processCreateEvent(tierId ftypes.RealmID, event *rpc.CreateAggregate, vg hangar.ValGroup) (hangar.ValGroup, error) {
	popts := event.GetOptions()
	aggId := ftypes.AggId(event.AggId)
	codecs := getCodecs(ftypes.AggType(popts.AggType))
	options := aggregate.FromProtoOptions(popts)
	for _, codec := range codecs {
		key := aggKey{tierId, aggId, codec}
		if v, ok := ndb.tables.Load(key); ok {
			if !v.(store.Table).Options().Equals(options) {
				return vg, fmt.Errorf("aggregate %d in tier %d already exists with different options", aggId, tierId)
			} else {
				continue
			}
		}
		field, err := encodeField(tierId, aggId, codec)
		if err != nil {
			return vg, fmt.Errorf("failed to encode hangar field for new aggregate %d in tier %d: %w", aggId, tierId, err)
		}
		rawopts, err := proto.Marshal(popts)
		if err != nil {
			return vg, fmt.Errorf("failed to byte-serialize aggregate options proto: %w", err)
		}
		vg.Fields = append(vg.Fields, field)
		vg.Values = append(vg.Values, rawopts)
		err = ndb.setupAggregateTable(tierId, aggId, codec, options)
		if err != nil {
			return vg, fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
		}
	}
	return vg, nil
}

func getCodecs(aggType ftypes.AggType) []rpc.AggCodec {
	return []rpc.AggCodec{rpc.AggCodec_V1}
}

func (ndb *NitrousDB) setupAggregateTable(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, options aggregate.Options) error {
	table, err := store.Make(tierId, aggId, codec, options, ndb.nos.Clock)
	if err != nil {
		return fmt.Errorf("failed to create aggregate store for {aggId: %d, tierId: %d, codec: %d} : %w", aggId, tierId, codec, err)
	}
	// Subscribe the aggregate store to the tailer for aggregate events.
	ndb.tailer.Subscribe(table)
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
	lag, err := ndb.tailer.GetLag()
	if err != nil {
		return 0, fmt.Errorf("error getting lag: %w", err)
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
