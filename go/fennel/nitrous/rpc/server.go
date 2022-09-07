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
	GetAggregatesLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "nitrous_get_aggregates_latency_ms",
		Help: "Server-side latency (in ms) of GetAggregateValues",
	})
	OK = status.New(codes.OK, "Success").Proto()
)

const (
	numConcurrentStreams = 128
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

	rateLimiter chan struct{}
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
		aggdb:       aggdb,
		inner:       grpcServer,
		rateLimiter: make(chan struct{}, numConcurrentStreams),
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

func (s *Server) processRequest(ctx context.Context, req *AggregateValuesRequest) (*AggregateValuesResponse, error) {
	start := time.Now()
	s.rateLimiter <- struct{}{}
	defer func() {
		GetAggregatesLatency.Observe(float64(time.Since(start).Milliseconds()))
		<-s.rateLimiter
	}()
	tierId := ftypes.RealmID(req.TierId)
	aggId := ftypes.AggId(req.AggId)
	codec := req.Codec
	kwargs := make([]value.Dict, len(req.Kwargs))
	var err error
	for i, kw := range req.Kwargs {
		kwargs[i], err = value.FromProtoDict(kw)
		if err != nil {
			s, _ := protojson.Marshal(kw)
			return nil, fmt.Errorf("error converting kwarg [%s] to value: %w", s, err)
		}
	}
	vals, err := s.aggdb.Get(ctx, tierId, aggId, codec, req.GetGroupkeys(), kwargs)
	if err != nil {
		return nil, fmt.Errorf("error getting aggregate values: %w", err)
	}
	pvalues := make([]*value.PValue, len(vals))
	for i, v := range vals {
		pv, err := value.ToProtoValue(v)
		if err != nil {
			return nil, fmt.Errorf("error converting value to proto: %w", err)
		}
		pvalues[i] = &pv
	}
	resp := &AggregateValuesResponse{Results: pvalues}
	return resp, nil
}

func (s *Server) GetAggregateValues(stream Nitrous_GetAggregateValuesServer) error {
	zap.L().Debug("Got new GetAggregateValues stream")
	streamCtx := stream.Context()
	_, cancelFn := context.WithCancel(streamCtx)
	defer cancelFn()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		ctx := context.Background()
		ctx = timer.WithTracing(ctx)
		resp, err := s.processRequest(ctx, req)
		if err != nil {
			s := status.Newf(codes.Internal, "error processing request: %v", err).Proto()
			stream.Send(&AggregateValuesResponse{
				Status: s,
			})
			continue
		} else {
			resp.Status = OK
			if err = stream.Send(resp); err != nil {
				zap.L().Warn("Error sending response to client", zap.Error(err))
				return err
			}
		}
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
	s.inner.Stop()
	s.aggdb.Stop()
}

func (s *Server) GetPollTimeout() time.Duration {
	return s.aggdb.GetPollTimeout()
}

func (s *Server) SetPollTimeout(timeout time.Duration) {
	s.aggdb.SetPollTimeout(timeout)
}
