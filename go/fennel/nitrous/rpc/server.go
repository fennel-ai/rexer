package rpc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/value"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type AggDB interface {
	Get(ctx context.Context, tierId ftypes.RealmID, aggId ftypes.AggId, codec AggCodec, groupkeys []string, kwargs []value.Dict) ([]value.Value, error)
	GetLag(ctx context.Context) (int, error)
	GetPollTimeout() time.Duration

	Stop()
	SetPollTimeout(time.Duration)
}

type Server struct {
	aggdb AggDB

	inner *grpc.Server
	// Embed UnimplementedNitrousServer for forward compatibility with future
	// RPC additions.
	UnimplementedNitrousServer
}

func FennelTracingInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	ctx = context.WithValue(ctx, timer.TraceKey{}, timer.TraceVal{})
	return handler(ctx, req)
}

func NewServer(aggdb AggDB) *Server {
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			FennelTracingInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
			otelgrpc.UnaryServerInterceptor(),
		)),
	)
	s := &Server{
		aggdb: aggdb,
		inner: grpcServer,
	}
	RegisterNitrousServer(grpcServer, s)
	// After all your registrations, make sure all of the Prometheus metrics are initialized.
	grpc_prometheus.Register(grpcServer)
	return s
}

func (s *Server) GetLag(ctx context.Context, _ *LagRequest) (*LagResponse, error) {
	lag, err := s.aggdb.GetLag(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting lag: %v", err)
	}
	return &LagResponse{
		Lag: uint64(lag),
	}, nil
}

func (s *Server) GetAggregateValues(ctx context.Context, req *AggregateValuesRequest) (*AggregateValuesResponse, error) {
	tierId := ftypes.RealmID(req.TierId)
	aggId := ftypes.AggId(req.AggId)
	codec := req.Codec
	kwargs := make([]value.Dict, len(req.Kwargs))
	var err error
	for i, kw := range req.Kwargs {
		kwargs[i], err = value.FromProtoDict(kw)
		if err != nil {
			s, _ := protojson.Marshal(kw)
			return nil, status.Errorf(codes.Internal, "error converting kwarg [%s] to value: %v", s, err)
		}
	}
	vals, err := s.aggdb.Get(ctx, tierId, aggId, codec, req.GetGroupkeys(), kwargs)
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
	return &AggregateValuesResponse{Results: pvalues}, nil
}

func (s *Server) Serve(listener net.Listener) error {
	if err := s.inner.Serve(listener); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("Server stopped before starting")
			return nil
		}
		return fmt.Errorf("failed to serve: %w", err)
	}
	return nil
}

func (s *Server) Stop() {
	s.inner.GracefulStop()
	s.aggdb.Stop()
}

func (s *Server) GetPollTimeout() time.Duration {
	return s.aggdb.GetPollTimeout()
}

func (s *Server) SetPollTimeout(timeout time.Duration) {
	s.aggdb.SetPollTimeout(timeout)
}
