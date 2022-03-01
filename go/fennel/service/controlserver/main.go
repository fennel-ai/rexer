package main

import (
	"log"
	"net/http"
	"time"

	"fennel/mothership"
	"fennel/mothership/controller/launchrequest"
)

const (
	serverAddress       = ":2475"
	endpoint            = "http://localhost:2425"
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

func main() {
	m, err := mothership.Create()
	if err != nil {
		log.Fatalf("Error creating mothership: %v", err)
	}

	server := createServer(serverAddress, endpoint)
	go pollLaunchRequests(m)

	log.Printf("starting http service on '%s'\n", server.address)
	log.Fatal(http.ListenAndServe(server.address, server))
}
