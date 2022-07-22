package main

import (
	"fennel/lib/timer"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server"
	"fennel/service/common"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/alexflint/go-arg"
	"go.uber.org/zap"
)

var flags struct {
	ListenPort uint32 `arg:"--listen-port,env:LISTEN_PORT" default:"3333" json:"listen_port,omitempty"`
	nitrous.NitrousArgs
	// Observability.
	common.PprofArgs
	common.PrometheusArgs
	timer.TracerArgs
}

func main() {
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Start a prometheus server.
	common.StartPromMetricsServer(flags.MetricsPort)
	// Start a pprof server to export the standard pprof endpoints.
	profiler := common.CreateProfiler(flags.PprofArgs)
	profiler.StartPprofServer()

	if flags.NitrousArgs.BackupNode {
		lastBackupTime := time.Now().Unix()
		var svr *server.NitrousDB = nil

		for {
			log.Printf("Creating NitrousDB instance")
			if svr == nil {
				n, err := nitrous.CreateFromArgs(flags.NitrousArgs)
				if err != nil {
					log.Fatalf("Failed to setup nitrous: %v", err)
				}

				// Initialize the db.
				svr, err := server.InitDB(n)
				if err != nil {
					n.Logger.Fatal("Failed to initialize db", zap.Error(err))
				}
				svr.Start()
				log.Printf("NitrousDB started")
			}
			log.Printf("Main procedure sleeping waiting for the next time to create backup...")
			time.Sleep(time.Minute)
			now := time.Now().Unix()
			if now > lastBackupTime+3600 {
				log.Printf("Going to create backup, stopping the DB")
				svr.Close()
				log.Printf("Creating the backup")
				_ = svr.Backup()
				log.Printf("Backup is done")
				svr = nil
				lastBackupTime = now
			}
		}
	} else {
		n, err := nitrous.CreateFromArgs(flags.NitrousArgs)
		if err != nil {
			log.Fatalf("Failed to setup nitrous: %v", err)
		}
		// Setup tracer provider (which exports remotely) if an endpoint is defined.
		// Otherwise a default tracer is used.
		if len(flags.TracerArgs.OtlpEndpoint) > 0 {
			err = timer.InitProvider(flags.TracerArgs.OtlpEndpoint)
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

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", flags.ListenPort))
		if err != nil {
			n.Logger.Fatal("Failed to listen", zap.Uint32("port", flags.ListenPort), zap.Error(err))
		}
		s := rpc.NewServer(svr)
		if err = s.Serve(lis); err != nil {
			n.Logger.Fatal("Server terminated / failed to start", zap.Error(err))
		}
	}
}
