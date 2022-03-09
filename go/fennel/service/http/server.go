package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"fennel/controller/action"
	aggregate2 "fennel/controller/aggregate"
	profile2 "fennel/controller/profile"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	actionlib "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	profilelib "fennel/lib/profile"
	"fennel/lib/query"
	"fennel/lib/value"
	"fennel/tier"
	"github.com/buger/jsonparser"
	"github.com/gorilla/mux"
	"google.golang.org/protobuf/proto"
)

const dedupTTL = 6 * time.Hour

func parse(req *http.Request, msg proto.Message) error {
	defer req.Body.Close()
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return err
	}
	return proto.Unmarshal(body, msg)
}

func readRequest(req *http.Request) ([]byte, error) {
	defer req.Body.Close()
	return ioutil.ReadAll(req.Body)
}

type server struct {
	tier tier.Tier
}

func (s server) setHandlers(router *mux.Router) {
	router.HandleFunc("/fetch", s.Fetch)
	router.HandleFunc("/get", s.GetProfile)
	router.HandleFunc("/set", s.SetProfile)
	router.HandleFunc("/log", s.Log)
	router.HandleFunc("/log_multi", s.LogMulti)
	router.HandleFunc("/get_multi", s.GetProfileMulti)
	router.HandleFunc("/query", s.Query)
	router.HandleFunc("/store_aggregate", s.StoreAggregate)
	router.HandleFunc("/retrieve_aggregate", s.RetrieveAggregate)
	router.HandleFunc("/deactivate_aggregate", s.DeactivateAggregate)
	router.HandleFunc("/aggregate_value", s.AggregateValue)
	router.HandleFunc("/batch_aggregate_value", s.BatchAggregateValue)
	router.HandleFunc("/get_operators", s.GetOperators)

	// for any requests starting with /debug, hand the control to default servemux
	// needed to enable pprof
	router.PathPrefix("/debug/").Handler(http.DefaultServeMux)
}

func (m server) Log(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var a actionlib.Action
	if err := json.Unmarshal(data, &a); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	dedupKey, err := jsonparser.GetString(data, "DedupKey")
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// If dedupKey is non-empty, request is ignored if dedupKey is already present in redis.
	// Try to set if it does not exist. If it succeeds, proceed normally.
	// Otherwise, drop request.
	if dedupKey != "" {
		ok, err := m.tier.Redis.SetNX(req.Context(), dedupKey, 1, dedupTTL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("Error: %v", err)
			return
		}
		if !ok {
			return
		}
	}
	// fwd to controller
	if err = action.Insert(req.Context(), m.tier, a); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	// nothing to do on successful call :)
}

func (m server) LogMulti(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("error: %v; no action was logged", err)
		return
	}
	var actions []actionlib.Action
	var dedupItems []struct{ DedupKey string }
	if err := json.Unmarshal(data, &actions); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v; no action was logged", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	if err := json.Unmarshal(data, &dedupItems); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v; no action was logged", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var batch []actionlib.Action
	var keys []string
	var vals []interface{}
	var ttls []time.Duration
	var ids []int
	for i, d := range dedupItems {
		if d.DedupKey == "" {
			// If dedup key is empty, add action to batch directly
			batch = append(batch, actions[i])
		} else {
			// otherwise, store them for duplication check
			keys = append(keys, d.DedupKey)
			vals = append(vals, 1)
			ttls = append(ttls, dedupTTL)
			ids = append(ids, i)
		}
	}
	// Check for dedup with a pipeline
	ok, err := m.tier.Redis.SetNXPipelined(req.Context(), keys, vals, ttls)
	for i, _ := range ok {
		if ok[i] {
			// If dedup key of an action was not set, add to batch
			batch = append(batch, actions[ids[i]])
		}
	}
	// fwd to controller
	if err = action.BatchInsert(req.Context(), m.tier, batch); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	// nothing to do on successful call :)
}

func (m server) Fetch(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var request actionlib.ActionFetchRequest
	if err := json.Unmarshal(data, &request); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// send to controller
	actions, err := action.Fetch(req.Context(), m.tier, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	ser, err := json.Marshal(actions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m server) GetProfile(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var request profilelib.ProfileItem
	if err := json.Unmarshal(data, &request); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// send to controller
	val, err := profile2.Get(req.Context(), m.tier, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	if val == nil {
		// no error but no value to return either, so we just write nothing and client knows that
		// empty response means no value
		fmt.Fprintf(w, "")
		return
	}
	// now serialize value to JSON
	valSer, err := value.ToJSON(val)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(valSer)
}

// TODO: add some locking etc to ensure that if two requests try to modify
// the same key/value, we don't Run into a race condition
func (m server) SetProfile(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var request profilelib.ProfileItem
	if err := json.Unmarshal(data, &request); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// send to controller
	if err = profile2.Set(req.Context(), m.tier, request); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
}

func (m server) GetProfileMulti(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var request profilelib.ProfileFetchRequest
	if err := json.Unmarshal(data, &request); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// send to controller
	profiles, err := profile2.GetMulti(req.Context(), m.tier, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	ser, err := json.Marshal(profiles)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m server) Query(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	tree, args, err := query.FromBoundQueryJSON(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// execute the tree
	i := interpreter.NewInterpreter(bootarg.Create(m.tier))
	ret, err := i.Eval(tree, args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	ser, err := value.ToJSON(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m server) StoreAggregate(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var agg aggregate.Aggregate
	if err := json.Unmarshal(data, &agg); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// call controller
	if err = aggregate2.Store(req.Context(), m.tier, agg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
}

func (m server) RetrieveAggregate(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var aggReq struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(data, &aggReq); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// call controller
	ret, err := aggregate2.Retrieve(req.Context(), m.tier, ftypes.AggName(aggReq.Name))
	if err == aggregate.ErrNotFound {
		// we don't throw an error, just return empty response
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	// to send ret back, marshal to json and then write it back
	ser, err := json.Marshal(&ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m server) DeactivateAggregate(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var aggReq struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(data, &aggReq); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	err = aggregate2.Deactivate(req.Context(), m.tier, ftypes.AggName(aggReq.Name))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
}

func (m server) AggregateValue(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var getAggValue aggregate.GetAggValueRequest
	if err := json.Unmarshal(data, &getAggValue); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	// call controller
	ret, err := aggregate2.Value(req.Context(), m.tier, getAggValue.AggName, getAggValue.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	// marshal ret and then write it back
	ser, err := value.ToJSON(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m server) BatchAggregateValue(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	var getAggValueList []aggregate.GetAggValueRequest
	if err := json.Unmarshal(data, &getAggValueList); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	ret, err := aggregate2.BatchValue(req.Context(), m.tier, getAggValueList)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	ser, err := value.ToJSON(value.List(ret))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(ser)
}

func (m server) GetOperators(w http.ResponseWriter, req *http.Request) {
	_, err := readRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}
	data, err := operators.GetOperatorsJSON()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error: %v", err)
		return
	}
	w.Write(data)
}
