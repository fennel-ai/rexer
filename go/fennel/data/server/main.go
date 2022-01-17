package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"fennel/data/lib"
	"fennel/data/server/actions"
	"google.golang.org/protobuf/proto"
)

var producer actions.ActionProducer
var consumer actions.ActionConsumer

func init() {
	ch := make(chan *lib.ProtoAction, 10)
	producer = actions.NewLocalActionProducer(ch)
	consumer = actions.NewLocalActionConsumer(ch)

	// NOTE: This is disabled right now since kafka topic deletion does not
	// immediately delete the topic, and subsequent CreateTopic call will fail.

	// kafkaClientConfig := &kafka.ClientConfig{
	// 	BootstrapServer: "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092",
	// 	Username:        "DXGQTONRSCJ7YC2W",
	// 	Password:        "s1TAmKoJ7WnbJusVMPlgvKbYszD6lE50789bM1Y6aTlJNtRjThhhPR8+LeaY6Mqi",
	// }
	// var topicName string
	// var err error
	// topicName, err = kafkaClientConfig.SetupTestTopic()
	// if err != nil {
	// 	panic(err)
	// }
	// groupName := "test-group"
	// producer, err = kafkaClientConfig.NewActionProducer(topicName)
	// if err != nil {
	// 	panic(err)
	// }
	// consumer, err = kafkaClientConfig.NewActionConsumer(groupName, topicName)
	// if err != nil {
	// 	panic(err)
	// }
}

func Log(w http.ResponseWriter, req *http.Request) {
	var pa lib.ProtoAction
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
	action := lib.FromProtoAction(&pa)
	err = action.Validate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now we know that this is a valid request, so let's store this in kafka
	err = producer.LogAction(&pa)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// if there is no error, we don't need to write anything back
}

// TailActions reads a single message from Kafka and logs it in the database
func (controller MainController) TailActions() error {
	// msg, err := kafka.ReadActionMessage()
	// //fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	// // validate this message and if valid, write it in DBConnection
	// var pa lib.ProtoAction

	// err = proto.Unmarshal(msg.Value, &pa)
	// if err != nil {
	// 	return err
	// }
	pa, err := consumer.ReadActionMessage()
	if err != nil {
		return err
	}
	action := lib.FromProtoAction(pa)
	err = action.Validate()
	if err != nil {
		return err
	}
	// Now we know that this is a valid action and a db call will be made
	// if timestamp isn't set explicitly, we set it to current time
	if action.Timestamp == 0 {
		action.Timestamp = lib.Timestamp(time.Now().Unix())
	}
	_, err = controller.actionTable.actionDBInsert(action)
	if err != nil {
		return err
	}
	return nil
}

func (controller MainController) Fetch(w http.ResponseWriter, req *http.Request) {
	var protoRequest lib.ProtoActionFetchRequest
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
	request := lib.FromProtoActionFetchRequest(&protoRequest)

	// now we know that this is a valid request, so let's make a db call
	actions, err := controller.actionTable.actionDBGet(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	actionList := lib.ToProtoActionList(actions)
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
	mux.HandleFunc("/log", Log)
	mux.HandleFunc("/rate", controller.Rate)
	server.Handler = mux
	go func() {
		defer serverWG.Done() // let main know we are done cleaning up

		log.Printf("starting http server on %s...", server.Addr)
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
		}
	}()

	// and other goroutines will run minutely crons to aggregate counters
	for {
		aggregate(controller)
		// now sleep for a minute
		time.Sleep(time.Minute)
	}
}
