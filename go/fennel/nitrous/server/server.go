package server

import (
	"context"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server/tailer"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AggDB interface {
	Get(ctx context.Context, tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, duration uint32, groupkeys []string) ([]value.Value, error)
}

type Server struct {
	aggdb  AggDB
	tailer *tailer.Tailer

	// Embed UnimplementedNitrousServer for forward compatibility with future
	// RPC additions.
	rpc.UnimplementedNitrousServer
}

var _ rpc.NitrousServer = &Server{}

func NewServer(aggdb AggDB, tailer *tailer.Tailer) *Server {
	return &Server{
		aggdb:  aggdb,
		tailer: tailer,
	}
}

func (s *Server) GetLag(ctx context.Context, _ *rpc.LagRequest) (*rpc.LagResponse, error) {
	lag, err := s.tailer.GetLag()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting lag: %v", err)
	}
	return &rpc.LagResponse{
		Lag: uint64(lag),
	}, nil
}

func (s *Server) GetAggregateValues(ctx context.Context, req *rpc.AggregateValuesRequest) (*rpc.AggregateValuesResponse, error) {
	tierId := ftypes.RealmID(req.TierId)
	aggId := ftypes.AggId(req.AggId)
	codec := req.Codec
	vals, err := s.aggdb.Get(ctx, tierId, aggId, codec, req.GetDuration(), req.GetGroupkeys())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting aggregate %d for tier %d with codec %d: %v", aggId, tierId, codec, err)
	}
	pvalues := make([]*value.PValue, len(vals))
	for i, v := range vals {
		pv, err := value.ToProtoValue(v)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error converting value to proto: %v", err)
		}
		pvalues[i] = &pv
	}
	return &rpc.AggregateValuesResponse{Results: pvalues}, nil
}
