package server

import (
	"context"
	"errors"
	"sync"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	rpc "fennel/nitrous/rpc/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	EEXISTS = errors.New("handler for aggregate already exists")
)

type aggKey struct {
	tierId ftypes.RealmID
	aggId  ftypes.AggId
	codec  rpc.AggCodec
}

type AggregateStore interface {
	Get(ctx context.Context, duration uint32, keys []string) ([]value.Value, error)
}

type Server struct {
	handlers map[aggKey]AggregateStore
	mu       sync.Mutex

	// Embed UnimplementedNitrousServer for forward compatibility with future
	// RPC additions.
	rpc.UnimplementedNitrousServer
}

var _ rpc.NitrousServer = &Server{}

func NewServer() *Server {
	return &Server{
		handlers: make(map[aggKey]AggregateStore),
	}
}

func (s *Server) RegisterHandler(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, handler AggregateStore) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Inserting directly in s.handlers would require us to take a write
	// mutex to prevent race conditions with simultaneous readers. To avoid
	// making the common read path slow, we instead make the update path slow
	// by copying the current map, inserting the new handler, and replacing it
	// with an updated map.
	newHandlers := make(map[aggKey]AggregateStore, len(s.handlers)+1)
	newKey := aggKey{tierId, aggId, codec}
	for k, v := range s.handlers {
		if k == newKey {
			return EEXISTS
		}
		newHandlers[k] = v
	}
	newHandlers[newKey] = handler
	s.handlers = newHandlers
	return nil
}

func (s *Server) GetAggregateValues(ctx context.Context, req *rpc.AggregateValuesRequest) (*rpc.AggregateValuesResponse, error) {
	tierId := ftypes.RealmID(req.TierId)
	aggId := ftypes.AggId(req.AggId)
	codec := req.Codec
	aggKey := aggKey{tierId, aggId, codec}
	handler, ok := s.handlers[aggKey]
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "aggregate %d not found for tier %d with codec %d", aggId, tierId, codec)
	}
	vals, err := handler.Get(ctx, req.GetDuration(), req.GetGroupkeys())
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
