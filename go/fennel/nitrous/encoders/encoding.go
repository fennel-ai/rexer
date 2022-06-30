package encoders

import (
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server/store"
	"fennel/nitrous/server/tailer"
	"fennel/plane"
)

type TailingStore interface {
	tailer.EventProcessor
	store.AggregateStore
}

type Encoder interface {
	NewStore(plane plane.Plane, tierId ftypes.RealmID, aggId ftypes.AggId, options aggregate.Options) (TailingStore, error)
}

func Get(codec rpc.AggCodec) Encoder {
	switch codec {
	case rpc.AggCodec_V1:
		return V1Encoder{}
	default:
		return nil
	}
}
