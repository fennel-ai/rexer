package rpc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc/keepalive"

	"fennel/lib/arena"
	"fennel/lib/utils/parallel"

	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/value"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	GetAggregatesLatency = promauto.NewSummary(prometheus.SummaryOpts{
		Name: "nitrous_get_aggregates_latency_ms",
		Help: "Server-side latency (in ms) of GetAggregateValues",
		// Track quantiles within small error
		Objectives: map[float64]float64{
			0.25: 0.075,
			0.50: 0.05,
			0.75: 0.025,
			0.90: 0.01,
			0.95: 0.005,
			0.99: 0.001,
			0.999: 0.0001,
			0.9999: 0.00001,
		},
		// Time window now is 30 seconds wide, defaults to 10m
		//
		// NOTE: we configure this > the lowest scrape interval configured for prometheus job
		MaxAge: 30 * time.Second,
		// we slide the window every 6 (= 30 / 5 ) seconds
		AgeBuckets: 5,
	})
	OK = status.New(codes.OK, "Success").Proto()
)

const (
	numConcurrentStreams = 128
)

type AggDB interface {
	Get(ctx context.Context, tierId ftypes.RealmID, aggId ftypes.AggId, codec AggCodec, groupkeys []string, kwargs []value.Dict, ret []value.Value) error
	GetLag() (int, error)
	GetBinlogPollTimeout() time.Duration

	Stop()
	SetBinlogPollTimeout(time.Duration)
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
		// Default keepalive parameters ensure that a client connection, from the server side, established for a large
		// enough amount of time.
		//
		// This is required to allow clients to valid keepalive connections - server closes connections for any client
		// who violate this
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			// this is configured < `Time` on the client side (time after which the client start sending keepalive
			// requests)
			MinTime: 5 * time.Second,
			// again same as the client - permitting the client to send keepalive even if there are no active RPCs
			PermitWithoutStream: true,
		}),
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

func (s *Server) GetLag(_ context.Context, _ *LagRequest) (*LagResponse, error) {
	lag, err := s.aggdb.GetLag()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting lag: %v", err)
	}
	return &LagResponse{
		Lag: uint64(lag),
	}, nil
}

func (s *Server) GetAggregateValues(ctx context.Context, req *AggregateValuesRequest) (*AggregateValuesResponse, error) {
	ctx = timer.WithTracing(ctx)
	start := time.Now()
	s.rateLimiter <- struct{}{}
	defer func() {
		GetAggregatesLatency.Observe(float64(time.Since(start).Milliseconds()))
		<-s.rateLimiter
	}()

	parallel.AcquireHighPriority("nitrous", 1.5*parallel.OneCPU) // considering there being multiple shards, assign more than OneCPU grabbing
	defer parallel.Release("nitrous", 1.5*parallel.OneCPU)
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
	gks := req.GetGroupkeys()
	vals := arena.Values.Alloc(len(gks), len(gks))
	defer arena.Values.Free(vals)
	err = s.aggdb.Get(ctx, tierId, aggId, codec, gks, kwargs, vals)
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

func (s *Server) GetBinlogPollTimeout() time.Duration {
	return s.aggdb.GetBinlogPollTimeout()
}

func (s *Server) SetBinlogPollTimeout(timeout time.Duration) {
	s.aggdb.SetBinlogPollTimeout(timeout)
}
