package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"fennel/controller/action"
	aggregate2 "fennel/controller/aggregate"
	profile2 "fennel/controller/profile"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	actionlib "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	httplib "fennel/lib/http"
	profilelib "fennel/lib/profile"
	"fennel/lib/query"
	"fennel/lib/value"
	"fennel/plane"

	"google.golang.org/protobuf/proto"
)

type holder struct {
	plane plane.Plane
}

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
		log.Printf("Error: %v", err)
		return
	}
	a := actionlib.FromProtoAction(&pa)
	// fwd to controller

	aid, err := action.Insert(m.plane, a)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	// write the actionID back
	fmt.Fprintf(w, fmt.Sprintf("%d", aid))
}

func (m holder) Fetch(w http.ResponseWriter, req *http.Request) {
	var protoRequest actionlib.ProtoActionFetchRequest
	if err := parse(req, &protoRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	request := actionlib.FromProtoActionFetchRequest(&protoRequest)
	// send to controller
	actions, err := action.Fetch(m.plane, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	actionList := actionlib.ToProtoActionList(actions)
	ser, err := proto.Marshal(actionList)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m holder) GetProfile(w http.ResponseWriter, req *http.Request) {
	var protoReq profilelib.ProtoProfileItem
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	request, err := profilelib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// send to controller
	val, err := profile2.Get(m.plane, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	if val == nil {
		// no error but no value to return either, so we just write nothing and client knows that
		// empty response means no value
		fmt.Fprintf(w, string(""))
		return
	}
	// now convert value to proto and serialize it
	pval, err := value.ToProtoValue(val)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	valueSer, err := proto.Marshal(&pval)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(valueSer)
}

// TODO: add some locking etc to ensure that if two requests try to modify
// the same key/value, we don't Run into a race condition
func (m holder) SetProfile(w http.ResponseWriter, req *http.Request) {
	var protoReq profilelib.ProtoProfileItem
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	request, err := profilelib.FromProtoProfileItem(&protoReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// send to controller
	if err = profile2.Set(m.plane, request); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
}

func (m holder) GetProfiles(w http.ResponseWriter, req *http.Request) {
	var protoRequest profilelib.ProtoProfileFetchRequest
	if err := parse(req, &protoRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	request := profilelib.FromProtoProfileFetchRequest(&protoRequest)
	// send to controller
	profiles, err := profile2.GetProfiles(m.plane, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	profileList, err := profilelib.ToProtoProfileList(profiles)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	ser, err := proto.Marshal(profileList)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m holder) Query(w http.ResponseWriter, req *http.Request) {
	var protoAstWithDict query.ProtoAstWithDict
	if err := parse(req, &protoAstWithDict); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	tree, dict, err := query.FromProtoAstWithDict(&protoAstWithDict)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// execute the tree
	i := interpreter.NewInterpreter(bootarg.Create(m.plane))
	i.SetQueryArgs(dict)
	ret, err := tree.AcceptValue(i)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	pval, err := value.ToProtoValue(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	ser, err := proto.Marshal(&pval)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m holder) StoreAggregate(w http.ResponseWriter, req *http.Request) {
	var protoAgg aggregate.ProtoAggregate
	if err := parse(req, &protoAgg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	agg, err := aggregate.FromProtoAggregate(protoAgg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// call controller
	if err = aggregate2.Store(m.plane, agg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
}

func (m holder) RetrieveAggregate(w http.ResponseWriter, req *http.Request) {
	var protoReq aggregate.AggRequest
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// call controller
	ret, err := aggregate2.Retrieve(m.plane, ftypes.AggType(protoReq.AggType), ftypes.AggName(protoReq.AggName))
	if err == aggregate.ErrNotFound {
		// we don't throw an error, just return empty response
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	// to send ret back, we will convert it to proto, marshal it and then write it back
	protoRet, err := aggregate.ToProtoAggregate(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	ser, err := proto.Marshal(&protoRet)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m holder) AggregateValue(w http.ResponseWriter, req *http.Request) {
	var protoReq aggregate.ProtoGetAggValueRequest
	if err := parse(req, &protoReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	getAggValue, err := aggregate.FromProtoGetAggValueRequest(&protoReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// call controller
	ret, err := aggregate2.Value(m.plane, getAggValue.AggType, getAggValue.AggName, getAggValue.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	// marshal ret and then write it back
	ser, err := value.Marshal(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func setHandlers(controller holder, mux *http.ServeMux) {
	mux.HandleFunc("/fetch", controller.Fetch)
	mux.HandleFunc("/get", controller.GetProfile)
	mux.HandleFunc("/set", controller.SetProfile)
	mux.HandleFunc("/log", controller.Log)
	mux.HandleFunc("/get_profiles", controller.GetProfiles)
	mux.HandleFunc("/query", controller.Query)
	mux.HandleFunc("/store_aggregate", controller.StoreAggregate)
	mux.HandleFunc("/retrieve_aggregate", controller.RetrieveAggregate)
	mux.HandleFunc("/aggregate_value", controller.AggregateValue)
}

func main() {
	// spin up http service
	server := &http.Server{Addr: fmt.Sprintf(":%d", httplib.PORT)}
	mux := http.NewServeMux()
	plane, err := plane.CreateFromEnv()
	if err != nil {
		panic(fmt.Sprintf("Failed to setup plane connectors: %v", err))

	}
	controller := holder{plane}
	setHandlers(controller, mux)
	server.Handler = mux
	log.Printf("starting http service on %s...", server.Addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		// unexpected error. port in use?
		log.Fatalf("ListenAndServe(): %v", err)
	}
}
