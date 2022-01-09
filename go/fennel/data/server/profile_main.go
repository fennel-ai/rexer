package main

import (
	"bytes"
	"encoding/json"
	"fennel/data/lib"
	"fennel/value"
	"fmt"
	"net/http"
	"time"
)

func get(w http.ResponseWriter, req *http.Request) {
	var item lib.ProfileItemSer
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	err := json.NewDecoder(req.Body).Decode(&item)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if item.Oid == 0 || item.Otype == 0 || item.Key == "" {
		http.Error(w, "all of oid, otype, key need to be specified", http.StatusBadRequest)
		return
	}
	// now we know that this is a valid request, so let's make a db call
	valueSer, err := dbGet(item.Otype, item.Oid, item.Key, item.Version)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	fmt.Fprintf(w, string(valueSer))
}

// TODO: add some locking etc to ensure that if two requests try to modify
// the same key/value, we don't run into a race condition
func set(w http.ResponseWriter, req *http.Request) {
	var item lib.ProfileItemSer
	// Try to decode the request body into the struct. If there is an error,
	// respond to the client with the error message and a 400 status code.
	buf := new(bytes.Buffer)
	buf.ReadFrom(req.Body)
	err := json.Unmarshal(buf.Bytes(), &item)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if (item.Oid == 0) || (item.Otype == 0) || (item.Key == "") {
		http.Error(w, "all of oid, otype, key need to be specified", http.StatusBadRequest)
		return
	}
	// verify that item.Value is valid and can be unmarshalled into a real value
	_, err = value.UnmarshalJSON(item.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Now we know that this is a valid request and a db call will be made
	// if version isn't set explicitly, we set it to current time
	if item.Version == 0 {
		item.Version = uint64(time.Now().Unix())
	}
	err = dbSet(item.Otype, item.Oid, item.Key, item.Version, item.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
}
