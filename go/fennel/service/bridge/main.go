package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"fennel/mothership"
	"fennel/mothership/controller/launchrequest"
	"github.com/alexflint/go-arg"
)

const (
	requestPollingDelay = time.Minute
)

func pollLaunchRequests(m mothership.Mothership) {
	for {
		log.Print("processing completed requests")
		err := launchrequest.ProcessCompletedRequests(m)
		if err != nil {
			log.Printf("Error polling: %v", err)
		}
		time.Sleep(requestPollingDelay)
	}
}

type BridgeArgs struct {
}

func main() {
	// Parse flags / environment variables.
	var flags struct {
		mothership.Args
	}
	arg.MustParse(&flags)
	endpoint := flag.String("BRIDGE_ENDPOINT", "http://localhost:2425", "server address to connect to")
	serverAddress := flag.String("BRIDGE_ADDRESS", ":2475", "address of the control server")
	flag.Parse()

	m, err := mothership.Create()
	if err != nil {
		log.Fatalf("Error creating mothership: %v", err)
	}

	server := createServer(*serverAddress, *endpoint)
	go pollLaunchRequests(m)

	log.Printf("starting http service on '%s'\n", server.address)
	log.Fatal(http.ListenAndServe(server.address, server))
}
