package main

import (
	"fmt"
	"log"
	"net"

	"fennel/lib/timer"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server"
	"fennel/service/common"

	"github.com/alexflint/go-arg"
	"go.uber.org/zap"
)

var flags struct {
	ListenPort uint32 `arg:"--listen-port,env:LISTEN_PORT" json:"listen_port"`
	nitrous.NitrousArgs
	// Observability.
	common.PprofArgs
	common.PrometheusArgs
	timer.TracerArgs
}

func main() {
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	n, err := nitrous.CreateFromArgs(flags.NitrousArgs)
	if err != nil {
		log.Fatalf("Failed to setup nitrous: %v", err)
	}

	// Setup tracer provider (which exports remotely) if an endpoint is defined.
	if len(flags.TracerArgs.OtlpEndpoint) > 0 {
		// Sampling ratio of 1.0 means 100% of traces are exported, unless this
		// is a part of a distributed trace, in which case sampling is pre-determined
		// by the parent trace being sampled.
		err = timer.InitProvider(flags.TracerArgs.OtlpEndpoint, timer.PathSampler{SamplingRatio: 1.0})
		if err != nil {
			log.Fatalf("Failed to setup tracing provider: %v", err)
		}
	}

	// Initialize the db.
	svr, err := server.InitDB(n)
	if err != nil {
		n.Logger.Fatal("Failed to initialize db", zap.Error(err))
	}
	svr.Start()

	// Start a prometheus server.
	common.StartPromMetricsServer(flags.MetricsPort)
	// Start a pprof server to export the standard pprof endpoints.
	profiler := common.CreateProfiler(flags.PprofArgs)
	profiler.StartPprofServer()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", flags.ListenPort))
	if err != nil {
		n.Logger.Fatal("Failed to listen", zap.Uint32("port", flags.ListenPort), zap.Error(err))
	}
	s := rpc.NewServer(svr)
	if err = s.Serve(lis); err != nil {
		n.Logger.Fatal("Server terminated / failed to start", zap.Error(err))
	}
}
