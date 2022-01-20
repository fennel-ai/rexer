package main

import (
	"context"
	"encoding/json"
	"fennel/controller/action"
	actionlib "fennel/lib/action"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"fennel/data/lib"
	"google.golang.org/protobuf/proto"
)

const (
	ACTIONLOG_TOPICNAME = "actionlog"
)

func (controller MainController) Log(w http.ResponseWriter, req *http.Request) {
	var pa actionlib.ProtoAction
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = proto.Unmarshal(body, &pa)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	action := actionlib.FromProtoAction(&pa)
	err = action.Validate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now we know that this is a valid request, so let's store this in kafka
	err = controller.instance.ActionProducer.Log(&pa)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// if there is no error, we don't need to write anything back
}

// TailActions reads a single message from Kafka and logs it in the database
func (controller MainController) TailActions() error {
	pa := actionlib.ProtoAction{}
	err := controller.instance.ActionConsumer.Read(&pa)
	if err != nil {
		return err
	}
	a := actionlib.FromProtoAction(&pa)
	err = a.Validate()
	if err != nil {
		return err
	}
	// Now we know that this is a valid actionlib and a db call will be made
	// if timestamp isn't set explicitly, we set it to current time
	if a.Timestamp == 0 {
		a.Timestamp = actionlib.Timestamp(time.Now().Unix())
	}
	_, err = action.Insert(controller.instance, a)
	if err != nil {
		return err
	}
	return nil
}

func (controller MainController) Fetch(w http.ResponseWriter, req *http.Request) {
	var protoRequest actionlib.ProtoActionFetchRequest
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = proto.Unmarshal(body, &protoRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request := actionlib.FromProtoActionFetchRequest(&protoRequest)
	actions, err := action.Fetch(controller.instance, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	actionList := actionlib.ToProtoActionList(actions)
	ser, err := proto.Marshal(actionList)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, string(ser))
}

func (controller MainController) Count(w http.ResponseWriter, req *http.Request) {
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var protoRequest lib.ProtoGetCountRequest
	err = proto.Unmarshal(body, &protoRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request := lib.FromProtoGetCountRequest(&protoRequest)
	// now we know that this is a valid request, so let's make a db call
	count, err := controller.counterTable.counterGet(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	ser, err := json.Marshal(count)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not serialize count: %v", err.Error()), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, string(ser))
}

func (controller MainController) Rate(w http.ResponseWriter, req *http.Request) {
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var protoRequest lib.ProtoGetRateRequest
	err = proto.Unmarshal(body, &protoRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request := lib.FromProtoGetRateRequest(&protoRequest)
	// now we know that this is a valid request, so let's make a db call
	numRequest := lib.GetCountRequest{CounterType: request.NumCounterType, Window: request.Window, Key: request.NumKey, Timestamp: request.Timestamp}
	numCount, err := controller.counterTable.counterGet(numRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	denRequest := lib.GetCountRequest{CounterType: request.DenCounterType, Window: request.Window, Key: request.DenKey, Timestamp: request.Timestamp}
	denCount, err := controller.counterTable.counterGet(denRequest)
	rate := Wilson(numCount, denCount, request.LowerBound)
	ser, err := json.Marshal(rate)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not serialize rate: %v", err.Error()), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, string(ser))
}

var server *http.Server
var serverWG sync.WaitGroup

func serve(controller MainController) {
	server = &http.Server{Addr: fmt.Sprintf(":%d", lib.PORT)}
	serverWG = sync.WaitGroup{}
	serverWG.Add(1)
	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", controller.Fetch)
	mux.HandleFunc("/count", controller.Count)
	mux.HandleFunc("/get", controller.get)
	mux.HandleFunc("/set", controller.set)
	mux.HandleFunc("/log", controller.Log)
	mux.HandleFunc("/rate", controller.Rate)
	server.Handler = mux
	go func() {
		defer serverWG.Done() // let main know we are done cleaning up

		log.Printf("starting http server on %s...", server.Addr)
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

func aggregate(controller MainController) {
	wg := sync.WaitGroup{}
	wg.Add(len(counterConfigs))
	for ct, _ := range counterConfigs {
		go func(ct lib.CounterType) {
			defer wg.Done()
			err := controller.run(ct)
			if err != nil {
				log.Printf("Error found in aggregate for counter type: %v. Err: %v", ct, err)
			}
		}(ct)
	}
	wg.Wait()
}

func main() {
	controller, err := DefaultMainController()
	if err != nil {
		panic(err)
	}
	// one goroutine will run http server
	go serve(controller)

	// one goroutine will constantly scan kafka and write actions
	go func() {
		for {
			controller.TailActions()
			time.Sleep(time.Millisecond * 100)
		}
	}()

	// and other goroutines will run minutely crons to aggregate counters
	for {
		aggregate(controller)
		// now sleep for a minute
		time.Sleep(time.Minute)
	}
}
