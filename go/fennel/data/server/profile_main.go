package main

import (
	profilelib "fennel/lib/profile"
	"fennel/value"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"net/http"
)

func (controller MainController) get(w http.ResponseWriter, req *http.Request) {
	var protoReq profilelib.ProtoProfileItem
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
	request, err := profilelib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	valuePtr, err := controller.profile.Get(request)
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
// the same key/value, we don't run into a race condition
func (controller MainController) set(w http.ResponseWriter, req *http.Request) {
	var protoReq profilelib.ProtoProfileItem
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err = proto.Unmarshal(body, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request, err := profilelib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}
	if err = controller.profile.Set(request); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
}
