package main

import (
	"fennel/data/lib"
	"fennel/value"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"net/http"
	"time"
)

func get(w http.ResponseWriter, req *http.Request) {
	var protoReq lib.ProtoProfileItem
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = proto.Unmarshal(body, &protoReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request, err := lib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	if err = request.Validate(); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	// now we know that this is a valid request, so let's make a db call
	valueSer, err := dbGet(request.OType, request.Oid, request.Key, request.Version)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// DB stores a serialized proto value - we just return it as it is
	fmt.Fprintf(w, string(valueSer))
}

// TODO: add some locking etc to ensure that if two requests try to modify
// the same key/value, we don't run into a race condition
func set(w http.ResponseWriter, req *http.Request) {
	var protoReq lib.ProtoProfileItem
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = proto.Unmarshal(body, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request, err := lib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	if err = request.Validate(); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	// TODO: reuse proto request's pvalue instead of creating a new one from scratch
	pval, err := value.ToProtoValue(request.Value)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadGateway)
		return
	}
	valSer, err := proto.Marshal(&pval)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadGateway)
		return
	}
	// Now we know that this is a valid request and a db call will be made
	// if version isn't set explicitly, we set it to current time
	if request.Version == 0 {
		request.Version = uint64(time.Now().Unix())
	}
	if err = dbSet(request.OType, request.Oid, request.Key, request.Version, valSer); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
}
