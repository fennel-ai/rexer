package main

import (
	"bytes"
	"encoding/json"
	"fennel/actionlog/lib"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

var inited bool = false

func init() {
	if !inited {
		dbInit()
		inited = false
	}
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

// Log logs the given action in the database after some validation
// if timestamp isn't specified, current timestamp is used
// if the action succeeds, returns action ID of the newly logged action
// TODO: add some locking etc to ensure that if two requests try to modify
// the same key/value, we don't run into a race condition
func Log(w http.ResponseWriter, req *http.Request) {
	var action lib.Action
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	buf := new(bytes.Buffer)
	buf.ReadFrom(req.Body)
	err := json.Unmarshal(buf.Bytes(), &action)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = action.Validate()
	if err != nil {
		http.Error(w, fmt.Sprintf("can not log invalid action: %v", err), http.StatusBadRequest)
		return
	}
	// Now we know that this is a valid request and a db call will be made
	// if timestamp isn't set explicitly, we set it to current time
	if action.Timestamp == 0 {
		action.Timestamp = lib.Timestamp(time.Now().Unix())
	}
	actionID, err := actionDBInsert(action)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	ser, err := json.Marshal(actionID)
	if err != nil {
		http.Error(w, fmt.Sprintf("server marshal error: %v", err.Error()), http.StatusBadGateway)
		return
	}
	w.Write(ser)
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

func serve() {
	http.HandleFunc("/fetch", Fetch)
	http.HandleFunc("/log", Log)
	http.HandleFunc("/count", Count)
	http.ListenAndServe(fmt.Sprintf(":%d", lib.PORT), nil)
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

	// and other goroutines will run minutely crons
	for {
		aggregate()
		// now sleep for a minute
		time.Sleep(time.Minute)
	}
	dbShutdown()
}
