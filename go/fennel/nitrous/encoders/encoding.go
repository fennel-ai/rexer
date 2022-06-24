package encoders

import (
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"
	"fennel/nitrous/server/tailer"
	"fennel/plane"
)

type TailingStore interface {
	tailer.EventProcessor
	server.AggregateStore
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
