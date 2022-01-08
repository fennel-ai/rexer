package main

import (
	"context"
	"encoding/json"
	"fennel/actionlog/lib"
	"fennel/instance"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

func init() {

	instance.Register(instance.DB, createCounterTables)
	instance.Register(instance.DB, createActionTable)
}

// Log reads a single message from Kafka and logs it in the database
func Log() error {
	msg, err := lib.KafkaActionConsumer().ReadMessage(-1)
	if err != nil {
		return err
	}
	//fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	// validate this message and if valid, write it in DB
	var action lib.Action
	err = json.Unmarshal(msg.Value, &action)
	if err != nil {
		return err
	}
	err = action.Validate()
	if err != nil {
		return err
	}
	// Now we know that this is a valid action and a db call will be made
	// if timestamp isn't set explicitly, we set it to current time
	if action.Timestamp == 0 {
		action.Timestamp = lib.Timestamp(time.Now().Unix())
	}
	_, err = actionDBInsert(action)
	if err != nil {
		return err
	}
	return nil
}

func Fetch(w http.ResponseWriter, req *http.Request) {
	var request lib.ActionFetchRequest
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	err := json.NewDecoder(req.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// now we know that this is a valid request, so let's make a db call
	actions, err := actionDBGet(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	ser, err := json.Marshal(actions)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not serialize actions: %v", err.Error()), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, string(ser))
}

func Count(w http.ResponseWriter, req *http.Request) {
	var request lib.GetCountRequest
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	err := json.NewDecoder(req.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// now we know that this is a valid request, so let's make a db call
	count, err := counterGet(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	//log.Printf("[AGGREGATOR] Read Count: %d\n", count)
	ser, err := json.Marshal(count)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not serialize count: %v", err.Error()), http.StatusBadGateway)
		return
	}
	//log.Printf("[AGGREGATOR] Count Ser: %s\n", ser)
	fmt.Fprintf(w, string(ser))
}

var server *http.Server
var serverWG sync.WaitGroup

func serve() {
	server = &http.Server{Addr: fmt.Sprintf(":%d", lib.PORT)}
	serverWG = sync.WaitGroup{}
	serverWG.Add(1)
	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", Fetch)
	mux.HandleFunc("/count", Count)
	server.Handler = mux
	go func() {
		defer serverWG.Done() // let main know we are done cleaning up

		// always returns error. ErrServerClosed on graceful close
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			// unexpected error. port in use?
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()
}

func shutDownServer() {
	log.Printf("shutting down http server")
	server.Shutdown(context.TODO())
	serverWG.Wait()
}

func aggregate() {
	wg := sync.WaitGroup{}
	wg.Add(len(counterConfigs))
	for ct, _ := range counterConfigs {
		go func(ct lib.CounterType) {
			defer wg.Done()
			err := run(ct)
			if err != nil {
				log.Printf("Error found in aggregate for counter type: %v. Err: %v", ct, err)
			}
		}(ct)
	}
	wg.Wait()
}

func main() {
	// one goroutine will run http server
	go serve()

	// one goroutine will constantly scan kafka and write actions
	go func() {
		for {
			Log()
		}
	}()

	// and other goroutines will run minutely crons to aggregate counters
	for {
		aggregate()
		// now sleep for a minute
		time.Sleep(time.Minute)
	}
}
