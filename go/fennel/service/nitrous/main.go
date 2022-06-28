package main

import (
	"fmt"
	"log"
	"net"

	"fennel/lib/timer"
	"fennel/nitrous"
	"fennel/plane"
	"fennel/service/common"

	"github.com/alexflint/go-arg"
	"go.uber.org/zap"
)

var flags struct {
	ListenPort uint32 `arg:"--listen-port,env:LISTEN_PORT" json:"listen_port"`
	plane.PlaneArgs
	// Observability.
	common.PprofArgs
	common.PrometheusArgs
	timer.TracerArgs
}

func main() {
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	plane, err := plane.CreateFromArgs(flags.PlaneArgs)
	if err != nil {
		log.Fatalf("Failed to setup plane: %v", err)
	}

	// Start a prometheus server.
	common.StartPromMetricsServer(flags.MetricsPort)
	// Start a pprof server to export the standard pprof endpoints.
	common.StartPprofServer(flags.PprofPort)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", flags.ListenPort))
	if err != nil {
		plane.Logger.Fatal("Failed to listen", zap.Uint32("port", flags.ListenPort), zap.Error(err))
	}

	if err = nitrous.StartServer(plane, lis); err != nil {
		plane.Logger.Fatal("Failed to start nitrous instance", zap.Error(err))
	}
}
