package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"fennel/hangar"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/nitrous/encoders"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"
	"fennel/nitrous/server/store"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

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

type AggDefsMgr struct {
	plane    plane.Plane
	tailer   *tailer.Tailer
	handlers map[aggKey]store.AggregateStore

	mu sync.Mutex
}

var _ server.AggDB = (*AggDefsMgr)(nil)

func NewAggDefsMgr(plane plane.Plane, tailer *tailer.Tailer) *AggDefsMgr {
	mgr := &AggDefsMgr{
		plane:    plane,
		tailer:   tailer,
		handlers: make(map[aggKey]store.AggregateStore),
	}
	tailer.Subscribe(mgr)
	return mgr
}

func (adm *AggDefsMgr) Identity() string {
	return "aggdefsmgr"
}

func (adm *AggDefsMgr) registerHandler(key aggKey, handler store.AggregateStore) error {
	adm.mu.Lock()
	defer adm.mu.Unlock()
	// Inserting directly in adm.handlers would require us to take a write
	// mutex to prevent race conditions with simultaneous readers. To avoid
	// making the common read path slow, we instead make the update path slow
	// by copying the current map, inserting the new handler, and replacing it
	// with an updated map.
	newHandlers := make(map[aggKey]store.AggregateStore, len(adm.handlers)+1)
	for k, v := range adm.handlers {
		if k == key {
			return errors.New("handler for aggregate already exists")
		}
		newHandlers[k] = v
	}
	newHandlers[key] = handler
	adm.handlers = newHandlers
	return nil
}

func (adm *AggDefsMgr) Process(ctx context.Context, ops []*rpc.NitrousOp) (keys []hangar.Key, vgs []hangar.ValGroup, err error) {
	for _, op := range ops {
		switch op.Type {
		case rpc.OpType_CREATE_AGGREGATE:
			event := op.GetCreateAggregate()
			popts := event.GetOptions()
			tierId := ftypes.RealmID(op.TierId)
			aggId := ftypes.AggId(event.AggId)
			ks, vs, err := adm.InitAggregate(tierId, aggId, popts)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to persist aggregate definition: %w", err)
			}
			keys = append(keys, ks...)
			vgs = append(vgs, vs...)
		}
	}
	return keys, vgs, nil
}

func (adm *AggDefsMgr) RestoreAggregates() error {
	// Get current set of aggregates for this plane.
	vgs, err := adm.plane.Store.GetMany([]hangar.KeyGroup{{Prefix: hangar.Key{Data: agg_table_key}}})
	if err != nil {
		return fmt.Errorf("failed to get aggregate definitions: %w", err)
	}
	if len(vgs) == 0 {
		// No aggregates defined yet.
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
		err = adm.initAggStore(tierId, aggId, codec, &popts)
		if err != nil {
			return fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
		}
	}
	return nil
}

func (adm *AggDefsMgr) InitAggregate(tierId ftypes.RealmID, aggId ftypes.AggId, popts *aggregate.AggOptions) ([]hangar.Key, []hangar.ValGroup, error) {
	// Get current set of aggregates for this plane.
	vgs, err := adm.plane.Store.GetMany([]hangar.KeyGroup{{Prefix: hangar.Key{Data: agg_table_key}}})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get aggregate definitions: %w", err)
	}
	var vg hangar.ValGroup
	if len(vgs) == 0 { // No aggregates defined yet.
		vg = hangar.ValGroup{Expiry: 0}
	} else {
		vg = vgs[0]
	}
	// TODO: Initialize the aggregate store only for codecs that are supported
	// for the given aggregate type.
	codecs := []rpc.AggCodec{rpc.AggCodec_V1}
	for _, codec := range codecs {
		field, err := encodeField(tierId, aggId, codec)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode hangar field for new aggregate %d in tier %d: %w", aggId, tierId, err)
		}
		rawopts, err := proto.Marshal(popts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to serialize aggregate options to json: %w", err)
		}
		// Check if this aggregate has already been defined for this tier.
		for i, f := range vg.Fields {
			if bytes.Equal(f, field) {
				currv := vg.Values[i]
				var curropts aggregate.AggOptions
				err = proto.Unmarshal(currv, &curropts)
				if err != nil {
					return nil, nil, fmt.Errorf("aggregate %d already defined for tier %d but failed to deserialize current options: %w", aggId, tierId, err)
				}
				if proto.Equal(&curropts, popts) {
					return nil, nil, nil
				} else {
					return nil, nil, fmt.Errorf("aggregate %d already defined for tier %d but current options != previous", aggId, tierId)
				}
			}
		}
		vg.Fields = append(vg.Fields, field)
		vg.Values = append(vg.Values, rawopts)
		err = adm.initAggStore(tierId, aggId, codec, popts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
		}
	}
	return []hangar.Key{{Data: agg_table_key}}, []hangar.ValGroup{vg}, nil
}

func (adm *AggDefsMgr) initAggStore(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, popts *aggregate.AggOptions) error {
	options := aggregate.FromProtoOptions(popts)
	encoder := encoders.Get(codec)
	if encoder == nil {
		return fmt.Errorf("no encoder for codec %d", codec)
	}
	ags, err := encoder.NewStore(adm.plane, tierId, aggId, options)
	if err != nil {
		return fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
	}
	// Subscribe the aggregate store to the tailer for aggregate events.
	// Also, register for gRPC.
	adm.tailer.Subscribe(ags)
	err = adm.registerHandler(aggKey{tierId, aggId, codec}, ags)
	if err != nil {
		return fmt.Errorf("failed to register aggregate handler for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
	}
	return nil
}

func (adm *AggDefsMgr) Get(ctx context.Context, tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, kwargs []value.Dict, groupkeys []string) ([]value.Value, error) {
	// Get the aggregate store for this aggregate.
	handler, ok := adm.handlers[aggKey{tierId, aggId, codec}]
	if !ok {
		return nil, fmt.Errorf("no handler for aggregate %d in tier %d with codec %d", aggId, tierId, codec)
	}
	return handler.Get(ctx, kwargs, groupkeys)
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
