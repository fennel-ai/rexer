package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"time"

	"fennel/controller/action"
	aggregate2 "fennel/controller/aggregate"
	"fennel/controller/mock"
	profile2 "fennel/controller/profile"
	"fennel/engine"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	actionlib "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	profilelib "fennel/lib/profile"
	"fennel/lib/query"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/tier"

	"github.com/buger/jsonparser"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"
)

const dedupTTL = 6 * time.Hour

var incomingActions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "incoming_actions_total",
		Help: "Total number of incoming actions.",
	},
	[]string{"path", "action_type"},
)

var totalActions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "actions_total",
		Help: "Total number of logged actions.",
	},
	[]string{"path", "action_type"},
)

var totalDedupedActions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "deduped_actions_total",
		Help: "Total number of deduped actions.",
	},
	[]string{"path", "action_type"},
)

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
	router.HandleFunc("/set_profiles", s.SetProfiles)
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
}

func (m server) Log(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var a actionlib.Action
	if err := json.Unmarshal(data, &a); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	err = a.Validate()
	if err != nil {
		handleBadRequest(w, "invalid action: ", err)
		return
	}
	incomingActions.WithLabelValues("log", string(a.ActionType)).Inc()

	dedupKey, err := jsonparser.GetString(data, "DedupKey")
	if err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// If dedupKey is non-empty, request is ignored if dedupKey is already present in redis.
	// Try to set if it does not exist. If it succeeds, proceed normally.
	// Otherwise, drop request.
	if dedupKey != "" {
		ok, err := m.tier.Redis.SetNX(req.Context(), dedupKey, 1, dedupTTL)
		if err != nil {
			handleInternalServerError(w, "", err)
			return
		}
		if !ok {
			totalDedupedActions.WithLabelValues("log", string(a.ActionType)).Inc()
			return
		}
	}
	// fwd to controller
	if err = action.Insert(req.Context(), m.tier, a); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	totalActions.WithLabelValues("log", string(a.ActionType)).Inc()
	// nothing to do on successful call :)
}

func (m server) LogMulti(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var actions []actionlib.Action
	var dedupItems []struct{ DedupKey string }
	if err := json.Unmarshal(data, &actions); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v; no action was logged", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}

	for _, a := range actions {
		incomingActions.WithLabelValues("log_multi", string(a.ActionType)).Inc()
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
	// TODO: Better variable name for ok
	ok, err := m.tier.Redis.SetNXPipelined(req.Context(), keys, vals, ttls)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}

	for i := range ok {
		if ok[i] {
			// If dedup key of an action was not set, add to batch
			batch = append(batch, actions[ids[i]])
		} else {
			totalDedupedActions.WithLabelValues("log_multi", string(actions[ids[i]].ActionType)).Inc()
		}
	}
	// fwd to controller
	if err = action.BatchInsert(req.Context(), m.tier, batch); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// increment metrics after successfully writing to the system
	for _, a := range batch {
		totalActions.WithLabelValues("log_multi", string(a.ActionType)).Inc()
	}
	// nothing to do on successful call :)
}

func (m server) Fetch(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var request actionlib.ActionFetchRequest
	if err := json.Unmarshal(data, &request); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// send to controller
	actions, err := action.Fetch(req.Context(), m.tier, request)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	ser, err := json.Marshal(actions)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	w.Write(ser)
}

func (m server) GetProfile(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var request profilelib.ProfileItem
	if err := json.Unmarshal(data, &request); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// send to controller
	val, err := profile2.Get(req.Context(), m.tier, request)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	if val == nil {
		// no error but no value to return either, so we just write nothing and client knows that
		// empty response means no value
		fmt.Fprintf(w, "")
		return
	}
	// now serialize value to JSON and write
	w.Write(value.ToJSON(val))
}

// TODO: add some locking etc to ensure that if two requests try to modify
// the same key/value, we don't Run into a race condition
func (m server) SetProfile(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var request profilelib.ProfileItem
	if err := json.Unmarshal(data, &request); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// send to controller
	if err = profile2.Set(req.Context(), m.tier, request); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
}

func (m server) SetProfiles(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var request []profilelib.ProfileItem
	if err := json.Unmarshal(data, &request); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// send to controller
	if err = profile2.SetMulti(req.Context(), m.tier, request); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
}

func (m server) GetProfileMulti(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var request profilelib.ProfileFetchRequest
	if err := json.Unmarshal(data, &request); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// send to controller
	profiles, err := profile2.GetMulti(req.Context(), m.tier, request)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	ser, err := json.Marshal(profiles)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	w.Write(ser)
}

func (m server) Query(w http.ResponseWriter, req *http.Request) {
	defer timer.Start(req.Context(), m.tier.ID, "query").Stop()
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	tree, args, mockData, err := query.FromBoundQueryJSON(data)
	if err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	if len(mockData.Profiles) > 0 {
		// set mock data
		mockID := rand.Int63()
		args.Set("__mock_id__", value.Int(mockID))
		mock.Store[mockID] = &mockData
		// unset mock data
		defer func() {
			if mockData.Profiles != nil {
				delete(mock.Store, mockID)
			}
		}()
	}
	// execute the tree
	executor := engine.NewQueryExecutor(bootarg.Create(m.tier))
	ret, err := executor.Exec(req.Context(), tree, args)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	w.Write(value.ToJSON(ret))
}

func (m server) StoreAggregate(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var agg aggregate.Aggregate
	if err := json.Unmarshal(data, &agg); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// call controller
	if err = aggregate2.Store(req.Context(), m.tier, agg); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
}

func (m server) RetrieveAggregate(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var aggReq struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(data, &aggReq); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// call controller
	ret, err := aggregate2.Retrieve(req.Context(), m.tier, ftypes.AggName(aggReq.Name))
	if err == aggregate.ErrNotFound {
		// we don't throw an error, just return empty response
		return
	} else if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// to send ret back, marshal to json and then write it back
	ser, err := json.Marshal(&ret)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	w.Write(ser)
}

func (m server) DeactivateAggregate(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var aggReq struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(data, &aggReq); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	err = aggregate2.Deactivate(req.Context(), m.tier, ftypes.AggName(aggReq.Name))
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
}

func (m server) AggregateValue(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var getAggValue aggregate.GetAggValueRequest
	if err := json.Unmarshal(data, &getAggValue); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// call controller
	ret, err := aggregate2.Value(req.Context(), m.tier, getAggValue.AggName, getAggValue.Key, getAggValue.Kwargs)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// marshal ret and then write it back
	w.Write(value.ToJSON(ret))
}

func (m server) BatchAggregateValue(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var getAggValueList []aggregate.GetAggValueRequest
	if err := json.Unmarshal(data, &getAggValueList); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	ret, err := aggregate2.BatchValue(req.Context(), m.tier, getAggValueList)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	w.Write(value.ToJSON(value.NewList(ret...)))
}

func (m server) GetOperators(w http.ResponseWriter, req *http.Request) {
	_, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	data, err := operators.GetOperatorsJSON()
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	w.Write(data)
}

func handleBadRequest(w http.ResponseWriter, errorPrefix string, err error) {
	http.Error(w, fmt.Sprintf("%s%v", errorPrefix, err), http.StatusBadRequest)
	log.Printf("Error: %v", err)
}

func handleInternalServerError(w http.ResponseWriter, errorPrefix string, err error) {
	http.Error(w, fmt.Sprintf("%s%v", errorPrefix, err), http.StatusInternalServerError)
	log.Printf("Error: %v", err)
}
