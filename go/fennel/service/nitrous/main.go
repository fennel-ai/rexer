package main

import (
	"fmt"
	"log"
	"net"
	"syscall"
	"time"

	"fennel/lib/timer"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server"
	"fennel/service/common"

	"github.com/alexflint/go-arg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var flags struct {
	ListenPort uint32 `arg:"--listen-port,env:LISTEN_PORT" json:"listen_port"`
	nitrous.NitrousArgs
	// Observability.
	common.PprofArgs
	common.PrometheusArgs
	timer.TracerArgs
	common.HealthCheckArgs
}

var fsStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "nitrous_fs_stats",
	Help: "Stats about nitrous file system",
}, []string{"metric"})

func reportDiskMetrics(path string, stop chan struct{}) {
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		for {
			select {
			case <-ticker.C:
				var stats syscall.Statfs_t
				if err := syscall.Statfs(path, &stats); err != nil {
					zap.L().Warn("failed to get statistics for the file system", zap.String("path", path), zap.Error(err))
					continue
				}
				fsStats.WithLabelValues("size").Set(float64(stats.Blocks * uint64(stats.Bsize)))
				fsStats.WithLabelValues("available").Set(float64(stats.Bavail * uint64(stats.Bsize)))
			case <-stop:
				return
			}
		}
	}()
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
		// A sampling ratio of 1.0 means 100% of traces are exported, unless this
		// is a part of a distributed trace, in which case sampling is pre-determined
		// by the parent trace being sampled.
		err = timer.InitProvider(flags.TracerArgs.OtlpEndpoint, timer.PathSampler{SamplingRatio: flags.TracerArgs.SamplingRatio})
		if err != nil {
			log.Fatalf("Failed to setup tracing provider: %v", err)
		}
	}

	// Initialize the db.
	svr, err := server.InitDB(n)
	if err != nil {
		zap.L().Fatal("Failed to initialize db", zap.Error(err))
	}
	svr.Start()

	// Start a prometheus server.
	common.StartPromMetricsServer(flags.MetricsPort)
	// Start health checker to export readiness and liveness state for the container running the server
	common.StartHealthCheckServer(flags.HealthPort)
	// Start a pprof server to export the standard pprof endpoints.
	profiler := common.CreateProfiler(flags.PprofArgs)
	profiler.StartPprofServer()

	stop := make(chan struct{}, 1)
	reportDiskMetrics(flags.GravelDir, stop)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", flags.ListenPort))
	if err != nil {
		zap.L().Fatal("Failed to listen", zap.Uint32("port", flags.ListenPort), zap.Error(err))
	}
	s := rpc.NewServer(svr)
	if err = s.Serve(lis); err != nil {
		zap.L().Fatal("Server terminated / failed to start", zap.Error(err))
	}
	stop <- struct{}{}
}
