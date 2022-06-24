package metadata

import (
	"bytes"
	"context"
	"fmt"

	"fennel/hangar"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/nitrous/encoders"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

	"google.golang.org/protobuf/proto"
)

var (
	agg_table_key = []byte("agg_table")
)

type AggDefsMgr struct {
	plane  plane.Plane
	tailer *tailer.Tailer
	svr    *server.Server
}

func NewAggDefsMgr(plane plane.Plane, tailer *tailer.Tailer, svr *server.Server) *AggDefsMgr {
	mgr := &AggDefsMgr{
		plane:  plane,
		tailer: tailer,
		svr:    svr,
	}
	tailer.Subscribe(mgr)
	return mgr
}

func (adm *AggDefsMgr) Identity() string {
	return "aggdefsmgr"
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
	for i, field := range vgs[0].Fields {
		_, tierId, aggId, codec, err := decodeField(field)
		if err != nil {
			return fmt.Errorf("failed to decode %s: %w", string(field), err)
		}
		var popts aggregate.AggOptions
		err = proto.Unmarshal(vgs[0].Values[i], &popts)
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
	if len(vgs) == 0 {
		// No aggregates defined yet.
		vgs = []hangar.ValGroup{{Expiry: -1}}
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
		for i, f := range vgs[0].Fields {
			if bytes.Equal(f, field) {
				currv := vgs[0].Values[i]
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
		vgs[0].Fields = append(vgs[0].Fields, field)
		vgs[0].Values = append(vgs[0].Values, rawopts)
		err = adm.initAggStore(tierId, aggId, codec, popts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
		}
	}
	return []hangar.Key{{Data: agg_table_key}}, vgs, nil
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
	err = adm.svr.RegisterHandler(tierId, aggId, codec, ags)
	if err != nil {
		return fmt.Errorf("failed to register aggregate handler for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
	}
	return nil
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
