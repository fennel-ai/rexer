package main

import (
	"context"
	"fennel/controller/action"
	counter2 "fennel/controller/counter"
	profile2 "fennel/controller/profile"
	"fennel/instance"
	actionlib "fennel/lib/action"
	"fennel/lib/counter"
	httplib "fennel/lib/http"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"google.golang.org/protobuf/proto"
)

type holder struct {
	instance instance.Instance
}

var server *http.Server
var serverWG sync.WaitGroup

func parse(req *http.Request, msg proto.Message) error {
	defer req.Body.Close()
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return err
	}
	return proto.Unmarshal(body, msg)
}

func (m holder) Log(w http.ResponseWriter, req *http.Request) {
	var pa actionlib.ProtoAction
	if err := parse(req, &pa); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a := actionlib.FromProtoAction(&pa)
	// fwd to controller

	aid, err := action.Insert(m.instance, a)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// write the actionID back
	fmt.Fprintf(w, fmt.Sprintf("%d", aid))
}

func (m holder) Fetch(w http.ResponseWriter, req *http.Request) {
	var protoRequest actionlib.ProtoActionFetchRequest
	if err := parse(req, &protoRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request := actionlib.FromProtoActionFetchRequest(&protoRequest)
	// send to controller
	actions, err := action.Fetch(m.instance, request)
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

func (m holder) Count(w http.ResponseWriter, req *http.Request) {
	var protoRequest counter.ProtoGetCountRequest
	if err := parse(req, &protoRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request := counter.FromProtoGetCountRequest(&protoRequest)
	// fwd to controller
	count, err := counter2.Count(m.instance, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, fmt.Sprintf("%d", count))
}

func (m holder) Rate(w http.ResponseWriter, req *http.Request) {
	var protoRequest counter.ProtoGetRateRequest
	if err := parse(req, &protoRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request := counter.FromProtoGetRateRequest(&protoRequest)
	// hit the controller
	rate, err := counter2.Rate(m.instance, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, fmt.Sprintf("%.9f", rate))
}

func (m holder) GetProfile(w http.ResponseWriter, req *http.Request) {
	var protoReq profilelib.ProtoProfileItem
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request, err := profilelib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	// send to controller
	valuePtr, err := profile2.Get(m.instance, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if valuePtr == nil {
		// no error but no value to return either, so we just write nothing and client knows that
		// empty response means no value
		fmt.Fprintf(w, string(""))
		return
	}
	// now convert value to proto and serialize it
	pval, err := value.ToProtoValue(*valuePtr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	valueSer, err := proto.Marshal(&pval)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, string(valueSer))
}

// TODO: add some locking etc to ensure that if two requests try to modify
// the same key/value, we don't Run into a race condition
func (m holder) SetProfile(w http.ResponseWriter, req *http.Request) {
	var protoReq profilelib.ProtoProfileItem
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request, err := profilelib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	// send to controller
	if err = profile2.Set(m.instance, request); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
}

func serve(controller holder) {
	server = &http.Server{Addr: fmt.Sprintf(":%d", httplib.PORT)}
	serverWG = sync.WaitGroup{}
	serverWG.Add(1)
	mux := http.NewServeMux()
	mux.HandleFunc("/fetch", controller.Fetch)
	mux.HandleFunc("/count", controller.Count)
	mux.HandleFunc("/get", controller.GetProfile)
	mux.HandleFunc("/set", controller.SetProfile)
	mux.HandleFunc("/log", controller.Log)
	mux.HandleFunc("/rate", controller.Rate)
	server.Handler = mux

	defer serverWG.Done() // let main know we are done cleaning up

	log.Printf("starting http service on %s...", server.Addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		// unexpected error. port in use?
		log.Fatalf("ListenAndServe(): %v", err)
	}

}

func shutDownServer() {
	log.Printf("shutting down http service")
	server.Shutdown(context.TODO())
	serverWG.Wait()
}

func main() {
	// TODO: don't use test instance here, instead create real instance using env variables
	instance, err := test.DefaultInstance()
	controller := holder{instance: instance}
	if err != nil {
		panic(err)
	}
	// spin up http service
	serve(controller)
}
