package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/value"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	GetAggregatesLatency = promauto.NewSummary(prometheus.SummaryOpts{
		Name: "nitrous_get_aggregates_latency_ms",
		Help: "Server-side latency (in ms) of GetAggregateValues",
		Objectives: map[float64]float64{
			0.25:   0.05,
			0.50:   0.05,
			0.75:   0.05,
			0.90:   0.05,
			0.95:   0.02,
			0.99:   0.01,
			0.999:  0.001,
			0.9999: 0.0001,
		},
	})
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
	ctx = timer.WithTracing(ctx)
	return handler(ctx, req)
}

func NewServer(aggdb AggDB) *Server {
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			FennelTracingInterceptor,
			grpc_prometheus.UnaryServerInterceptor,
			otelgrpc.UnaryServerInterceptor(),
			NewRateLimiter(5000),
		)),
	)
	s := &Server{
		aggdb: aggdb,
		inner: grpcServer,
	}
	RegisterNitrousServer(grpcServer, s)
	// After all your registrations, make sure all of the Prometheus metrics are initialized.
	grpc_prometheus.Register(grpcServer)
	// Enable latency histograms as per:
	// https://github.com/grpc-ecosystem/go-grpc-prometheus/blob/82c243799c991a7d5859215fba44a81834a52a71/README.md#histograms
	grpc_prometheus.EnableHandlingTimeHistogram()
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

func (s *Server) GetAggregateValues(stream Nitrous_GetAggregateValuesServer) error {
	zap.L().Debug("Got new GetAggregateValues stream")
	ctx := stream.Context()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		start := time.Now()
		tierId := ftypes.RealmID(req.TierId)
		aggId := ftypes.AggId(req.AggId)
		codec := req.Codec
		kwargs := make([]value.Dict, len(req.Kwargs))
		for i, kw := range req.Kwargs {
			kwargs[i], err = value.FromProtoDict(kw)
			if err != nil {
				s, _ := protojson.Marshal(kw)
				return status.Errorf(codes.Internal, "error converting kwarg [%s] to value: %v", s, err)
			}
		}
		vals, err := s.aggdb.Get(ctx, tierId, aggId, codec, req.GetGroupkeys(), kwargs)
		if err != nil {
			zap.L().Error("error getting aggregate values", zap.Error(err))
			stream.Send(&AggregateValuesResponse{})
			// return status.Errorf(codes.Internal, "error getting aggregate %d for tier %d with codec %d: %v", aggId, tierId, codec, err)
			continue
		}
		pvalues := make([]*value.PValue, len(vals))
		for i, v := range vals {
			pv, err := value.ToProtoValue(v)
			if err != nil {
				return status.Errorf(codes.Internal, "error converting value to proto: %v", err)
			}
			pvalues[i] = &pv
		}
		if err = stream.Send(&AggregateValuesResponse{Results: pvalues}); err != nil {
			return err
		}
		GetAggregatesLatency.Observe(float64(time.Since(start).Milliseconds()))
	}
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
