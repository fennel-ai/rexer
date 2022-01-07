package main

import (
	"encoding/json"
	"fennel/actionlog/lib"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var inited bool = false

// Kafka consumer
var kConsumer *kafka.Consumer

// TODO: find a cleaner way of doing this once init
func init() {
	if !inited {
		dbInit()
		initKafkaConsumer()
		inited = true
	}
}

func initKafkaConsumer() {
	var err error
	kConsumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		// connection configs.
		"bootstrap.servers": lib.KAFKA_BOOTSTRAP_SERVER,
		"security.protocol": lib.KAFKA_SECURITY_PROTOCOL,
		"sasl.mechanisms":   lib.KAFKA_SASL_MECHANISM,
		"sasl.username":     lib.KAFKA_USERNAME,
		"sasl.password":     lib.KAFKA_PASSWORD,

		// consumer specific configs.
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	kConsumer.SubscribeTopics([]string{lib.KAFKA_TOPIC}, nil)
}

// Log reads a single message from Kafka and logs it in the database
func Log() error {
	msg, err := kConsumer.ReadMessage(-1)
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

func serve() {
	http.HandleFunc("/fetch", Fetch)
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
	dbShutdown()
}
