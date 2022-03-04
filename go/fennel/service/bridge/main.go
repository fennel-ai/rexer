package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"fennel/mothership"
	"fennel/mothership/controller/launchrequest"

	"github.com/alexflint/go-arg"
)

const (
	requestPollingDelay = time.Minute
	dataPlaneEndpoint   = "http://http-server.fennel:2425"
)

func pollLaunchRequestStatus(m mothership.Mothership) {
	ticker := time.NewTicker(requestPollingDelay)
	defer ticker.Stop()
	for ; true; <-ticker.C {
		log.Print("processing completed requests")
		err := launchrequest.ProcessCompletedRequests(m)
		if err != nil {
			log.Printf("Error polling: %v", err)
		}
		time.Sleep(requestPollingDelay)
	}
}

type BridgeArgs struct {
	Port uint32 `arg:"--bridge-port,env:BRIDGE_PORT" default:"2475"`
}

func main() {
	// Parse flags / environment variables.
	var flags struct {
		mothership.MothershipArgs
		BridgeArgs
	}
	arg.MustParse(&flags)

	m, err := mothership.CreateFromArgs(&flags.MothershipArgs)
	if err != nil {
		log.Fatalf("Error creating mothership: %v", err)
	}

	server := createServer(flags.BridgeArgs.Port, dataPlaneEndpoint)
	go pollLaunchRequestStatus(m)

	address := fmt.Sprintf(":%d", server.port)
	log.Printf("starting http service on '%s'\n", address)
	log.Fatal(http.ListenAndServe(address, server))
}
