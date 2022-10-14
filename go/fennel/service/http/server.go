package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	_ "net/http/pprof"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"fennel/controller/usage"
	"fennel/redis"

	"fennel/controller/action"
	aggregate2 "fennel/controller/aggregate"
	connector2 "fennel/controller/data_integration"
	"fennel/controller/mock"
	"fennel/controller/modelstore"
	profile2 "fennel/controller/profile"
	query2 "fennel/controller/query"
	"fennel/engine"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	actionlib "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/data_integration"
	"fennel/lib/ftypes"
	profilelib "fennel/lib/profile"
	"fennel/lib/query"
	"fennel/lib/sagemaker"
	"fennel/lib/sql"
	"fennel/lib/timer"
	"fennel/lib/value"
	usagemodel "fennel/model/usage"
	"fennel/tier"

	usagelib "fennel/lib/usage"

	"github.com/Unleash/unleash-client-go/v3"
	"github.com/buger/jsonparser"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const dedupTTL = 6 * time.Hour

const EXT_REST_VERSION = "/v1"
const INT_REST_VERSION = "/internal/v1"

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

var totalUnleashQueryRequestsDropped = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "unleash_query_request_dropped",
		Help: "Total number of query requests dropped by unleash",
	},
)

func readRequest(req *http.Request) ([]byte, error) {
	defer req.Body.Close()
	return ioutil.ReadAll(req.Body)
}

type server struct {
	tier            tier.Tier
	usageController usage.UsageController
}

func (s server) Close() {
}

func NewServer(tier *tier.Tier, usageController usage.UsageController) *server {
	return &server{
		tier:            *tier,
		usageController: usageController,
	}
}

func (s *server) LimitFunc(ctx context.Context, rateLimit int64) bool {
	if rateLimit <= 0 {
		return true
	}
	endTime := uint64(s.tier.Clock.Now())
	startTime := usagelib.DailyFold(endTime)
	return func() int64 {
		counter, err := usagemodel.GetUsageCounters(ctx, s.tier, startTime, endTime)
		if err != nil {
			log.Printf("failed to get usage counters from db: %s", err)
			return 0
		}
		return int64(counter.Queries + counter.Actions)
	}() <= rateLimit
}

func (s *server) SetRateLimit(handler func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	log.Printf("request limit for tier is %d", s.tier.RequestLimit)
	return func(res http.ResponseWriter, req *http.Request) {
		if s.LimitFunc(req.Context(), s.tier.RequestLimit) {
			handler(res, req)
		} else {
			handleTooManyRequests(res, "request rejected: ", fmt.Errorf("number of requests in the day more than the limit of %d", s.tier.RequestLimit))
		}
	}
}

func (s server) setHandlers(router *mux.Router) {
	// OLDER END POINTS WILL BE DEPRECATED

	// Endpoints used by python client
	router.HandleFunc("/fetch", s.Fetch)
	router.HandleFunc("/get", s.GetProfile)
	router.HandleFunc("/set", s.SetProfile)
	router.HandleFunc("/set_profiles", s.SetProfiles)
	router.HandleFunc("/log", s.Log)
	router.HandleFunc("/log_multi", s.SetRateLimit(s.LogMulti))
	router.HandleFunc("/get_multi", s.GetProfileMulti)
	router.HandleFunc("/query", s.SetRateLimit(s.Query))
	router.HandleFunc("/store_query", s.StoreQuery)
	router.HandleFunc("/get_operators", s.GetOperators)
	router.HandleFunc("/run_query", s.SetRateLimit(s.RunQuery))

	// Endpoints used by aggregate
	router.HandleFunc("/store_aggregate", s.StoreAggregate)
	router.HandleFunc("/retrieve_aggregate", s.RetrieveAggregate)
	router.HandleFunc("/deactivate_aggregate", s.DeactivateAggregate)
	router.HandleFunc("/aggregate_value", s.AggregateValue)
	router.HandleFunc("/batch_aggregate_value", s.BatchAggregateValue)

	// Endpoints used by the model
	router.HandleFunc("/upload_model", s.UploadModel)
	router.HandleFunc("/delete_model", s.DeleteModel)
	router.HandleFunc("/enable_model", s.EnableModel)

	//--------------------------------Version Based Apis--------------------------------------------------
	// Format is <version>/<resource>/<verb>
	// ----------------------------------------/v1--------------------------------------------------------

	router.HandleFunc(INT_REST_VERSION+"/profiles", s.GetProfileMulti).Methods("GET")
	router.HandleFunc(INT_REST_VERSION+"/query_profiles", s.QueryProfiles).Methods("POST")
	router.HandleFunc(INT_REST_VERSION+"/profiles", s.SetProfiles).Methods("POST")
	router.HandleFunc(INT_REST_VERSION+"/log", s.SetRateLimit(s.LogMulti)).Methods("POST")

	router.HandleFunc(INT_REST_VERSION+"/query", s.SetRateLimit(s.Query))
	router.HandleFunc(INT_REST_VERSION+"/query/pandas", s.SetRateLimit(s.QueryPandas))
	router.HandleFunc(INT_REST_VERSION+"/query/store", s.StoreQuery).Methods("POST")

	// Endpoints used by aggregate
	router.HandleFunc(INT_REST_VERSION+"/aggregate", s.StoreAggregate).Methods("POST")
	router.HandleFunc(INT_REST_VERSION+"/aggregate", s.RetrieveAggregate).Methods("GET")
	router.HandleFunc(INT_REST_VERSION+"/aggregate", s.DeactivateAggregate).Methods("DELETE")
	router.HandleFunc(INT_REST_VERSION+"/aggregate/compute", s.BatchAggregateValue)
	router.HandleFunc(INT_REST_VERSION+"/aggregate/run", s.RunAggregate)

	// Endpoints used by the model
	router.HandleFunc(INT_REST_VERSION+"/model", s.UploadModel).Methods("POST")
	router.HandleFunc(INT_REST_VERSION+"/model", s.DeleteModel).Methods("DELETE")
	router.HandleFunc(INT_REST_VERSION+"/model/enable", s.EnableModel).Methods("POST")

	// Endpoints used for data integration
	router.HandleFunc(INT_REST_VERSION+"/connector", s.StoreConnector).Methods("POST")
	router.HandleFunc(INT_REST_VERSION+"/connector/disable", s.DisableConnector).Methods("POST")
	router.HandleFunc(INT_REST_VERSION+"/connector", s.DeleteConnector).Methods("DELETE")
	router.HandleFunc(INT_REST_VERSION+"/source", s.StoreSource).Methods("POST")
	router.HandleFunc(INT_REST_VERSION+"/source", s.DeleteSource).Methods("DELETE")

	// Misc endpoints
	router.HandleFunc(INT_REST_VERSION+"/operators", s.GetOperators).Methods("GET")

	// ----------------------------------External Endpoints-----------------------------------------------

	router.HandleFunc(EXT_REST_VERSION+"/actions", s.SetRateLimit(s.LogActions))
	router.HandleFunc(EXT_REST_VERSION+"/profiles", s.LogProfiles)
	router.HandleFunc(EXT_REST_VERSION+"/query", s.SetRateLimit(s.RunQuery))
	router.HandleFunc(EXT_REST_VERSION+"/usage_counters", s.GetusageCounters)
}

func constructDedupKey(dedupKey string, actionType ftypes.ActionType) string {
	// add action type to the `dedupKey`, so that the actions are deduplicated at the
	// granularity of action type
	//
	// NOTE: It is possible that the incoming requests could have handled this explicitly,
	// we add the action type as a sanity check
	var b strings.Builder
	b.WriteString(dedupKey)
	b.WriteString(":")
	b.WriteString(string(actionType))
	return b.String()
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
		ok, err := m.tier.Redis.SetNX(req.Context(), constructDedupKey(dedupKey, a.ActionType), 1, dedupTTL)
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
	m.usageController.IncCounter(&usagelib.UsageCountersProto{
		Actions: 1,
	})
	handleSuccessfulRequest(w)
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
			keys = append(keys, constructDedupKey(d.DedupKey, actions[i].ActionType))
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
		// if redis set command failed for whatever reason (this seem to be common when any one
		// of the shard is full), instead of dropping it, use it
		if ok[i] == redis.NotFoundSet || ok[i] == redis.Error {
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
	m.usageController.IncCounter(&usagelib.UsageCountersProto{
		Actions: uint64(len(actions)),
	})
	handleSuccessfulRequest(w)
}

func (m server) LogActions(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}

	actions, err := GetActionsFromRest(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v; no action was logged", err), http.StatusBadRequest)
		log.Printf("Error: %v", err)
		return
	}

	// fwd to controller
	if err = action.BatchInsert(req.Context(), m.tier, actions); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// increment metrics after successfully writing to the system
	for _, a := range actions {
		totalActions.WithLabelValues("log_multi", string(a.ActionType)).Inc()
	}

	m.usageController.IncCounter(&usagelib.UsageCountersProto{
		Actions: uint64(len(actions)),
	})
	handleSuccessfulRequest(w)
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
	_, _ = w.Write(ser)
}

func (m server) GetProfile(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var request profilelib.ProfileItemKey
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
	if val.Value == nil {
		// no error but no value to return either, so we just write nothing and client knows that
		// empty response means no value
		fmt.Fprintf(w, "")
		return
	}
	// now serialize value to JSON and write
	_, _ = w.Write(value.ToJSON(val.Value))
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
	handleSuccessfulRequest(w)
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
	handleSuccessfulRequest(w)
}

func (m server) LogProfiles(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	profiles, err := GetProfilesFromRest(data)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// send to controller
	if err = profile2.SetMulti(req.Context(), m.tier, profiles); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	handleSuccessfulRequest(w)
}

func (m server) QueryProfiles(w http.ResponseWriter, req *http.Request) {
	var err error
	var data []byte
	if data, err = readRequest(req); err != nil {
		handleBadRequest(w, "invalid request", err)
		return
	}
	form := profilelib.QueryRequest{
		Pagination: sql.NewPagination(),
	}
	if err = json.Unmarshal(data, &form); err != nil {
		handleBadRequest(w, "invalid request", fmt.Errorf("failed to parse query filter from json: %s", err))
		return
	}
	profiles, err := profile2.Query(req.Context(), m.tier, form.Otype, form.Oid, form.Pagination)
	if err != nil {
		handleInternalServerError(w, "invalid request: ", err)
		return
	}
	ser, err := json.Marshal(profiles)
	if err != nil {
		handleInternalServerError(w, "invalid request: ", err)
		return

	}
	_, _ = w.Write(ser)
}

func (m server) GetProfileMulti(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var request []profilelib.ProfileItemKey
	if err := json.Unmarshal(data, &request); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// send to controller
	profiles, err := profile2.GetBatch(req.Context(), m.tier, request)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}

	ser, err := json.Marshal(profiles)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	_, _ = w.Write(ser)
}

func (m server) Query(w http.ResponseWriter, req *http.Request) {
	// disable-query-calls is configured with random stickiness, which returns random true/false based on the
	// percentage configured.
	if unleash.IsEnabled("disable-query-calls") {
		totalUnleashQueryRequestsDropped.Inc()
		_, _ = w.Write([]byte("{}"))
		return
	}

	data, err := readRequest(req)
	cCtx, span := timer.Start(req.Context(), m.tier.ID, "server.Query")
	defer span.Stop()
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	_, querySpan := timer.Start(cCtx, m.tier.ID, "query.FromBoundQueryJSON")
	tree, args, mockData, err := query.FromBoundQueryJSON(data)
	querySpan.Stop()
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
	ret, err := executor.Exec(cCtx, tree, args)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	m.usageController.IncCounter(&usagelib.UsageCountersProto{Queries: 1})
	_, _ = w.Write(value.ToJSON(ret))

}

func runPandasQuery(queryStr, args, types string) (string, error) {
	cmd := exec.Command("python3", "service/http/transform_pandas.py", queryStr, args, types)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to execute python script: %w", err)
	}
	return string(out), nil
}

func (m server) QueryPandas(w http.ResponseWriter, req *http.Request) {
	// disable-query-calls is configured with random stickiness, which returns random true/false based on the
	// percentage configured.
	if unleash.IsEnabled("disable-query-calls") {
		totalUnleashQueryRequestsDropped.Inc()
		_, _ = w.Write([]byte("{}"))
		return
	}

	data, err := readRequest(req)
	_, span := timer.Start(req.Context(), m.tier.ID, "server.QueryPandas")
	defer span.Stop()
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	queryStr, err := jsonparser.GetString(data, "Query")
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	args, _, _, err := jsonparser.Get(data, "Args")
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	types, _, _, err := jsonparser.Get(data, "Types")
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}

	if ret, err := runPandasQuery(queryStr, string(args), string(types)); err != nil {
		handleInternalServerError(w, "", err)
		return
	} else {
		_, _ = w.Write([]byte(ret))
	}

	m.usageController.IncCounter(&usagelib.UsageCountersProto{Queries: 1})
}

func (m server) StoreQuery(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	name, err := jsonparser.GetString(data, "name")
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	qStr, err := jsonparser.GetString(data, "query")
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	q, err := query.FromString(qStr)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	_, err = query2.Insert(m.tier, name, q)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// if storing succeeds, just return empty response
	handleSuccessfulRequest(w)
}

func (m server) GetusageCounters(w http.ResponseWriter, req *http.Request) {
	startTimeStr := req.URL.Query().Get("start_time")
	endTimeStr := req.URL.Query().Get("end_time")
	var startTime, endTime uint64
	var err error

	// By default get the usage of the current hour.
	parseTime := func(tstr string) (uint64, error) {
		tUint, err := strconv.ParseUint(tstr, 10, 64)
		if err != nil {
			tTime, err := time.Parse(time.RFC3339, tstr)
			if err != nil {
				return 0, err
			}
			return uint64(tTime.Unix()), nil
		}
		return tUint, nil

	}

	if startTimeStr != "" {
		startTime, err = parseTime(startTimeStr)
		if err != nil {
			handleBadRequest(w, "failed to parse query param `start_time`, should be either epoch or in format "+time.RFC3339+" :", err)
			return
		}
	}
	if endTimeStr != "" {
		endTime, err = parseTime(endTimeStr)
		if err != nil {
			handleBadRequest(w, "failed to parse query param `end_time`, should be either epoch or in format "+time.RFC3339+" :", err)
			return
		}
	}
	// By default try the best effort of reporting billing for an hourly window.
	if startTime == 0 && endTime == 0 {
		startTime = usagelib.HourlyFold(uint64(m.tier.Clock.Now()))
		endTime = startTime + usagelib.HourInSeconds()
	} else if startTime == 0 {
		startTime = endTime - usagelib.HourInSeconds()
	} else if endTime == 0 {
		endTime = startTime + usagelib.HourInSeconds()
	}
	bc, err := usagemodel.GetUsageCounters(req.Context(), m.tier, startTime, endTime)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	var b []byte
	b, err = json.Marshal(bc)
	if err != nil {
		handleInternalServerError(w, "", err)
	}
	_, _ = w.Write(b)
}

func (m server) RunQuery(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	name, err := jsonparser.GetString(data, "name")
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	args := value.NewDict(nil)
	vData, vType, _, err := jsonparser.Get(data, "args")
	if err != nil && err != jsonparser.KeyPathNotFoundError {
		handleBadRequest(w, "", err)
		return
	}
	if err != jsonparser.KeyPathNotFoundError {
		v, err := value.ParseJSON(vData, vType)
		if err != nil {
			handleBadRequest(w, "", err)
			return
		}
		var ok bool
		args, ok = v.(value.Dict)
		if !ok {
			handleBadRequest(w, "", fmt.Errorf("error: expected 'args' to be a value.Dict but found: '%v'", v.String()))
			return
		}
	}
	tree, err := query2.Get(m.tier, name)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// execute the tree
	executor := engine.NewQueryExecutor(bootarg.Create(m.tier))
	ret, err := executor.Exec(req.Context(), tree, args)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	m.usageController.IncCounter(&usagelib.UsageCountersProto{Queries: 1})
	_, _ = w.Write(value.ToJSON(ret))
}

func (m server) StoreConnector(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}

	var conn data_integration.Connector
	if err := json.Unmarshal(data, &conn); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}

	if err := connector2.StoreConnector(req.Context(), m.tier, conn); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// if storing succeeds, just return empty response
}

func (m server) StoreSource(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}

	src, err := connector2.UnmarshalSource(data)
	if err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}

	if err := connector2.StoreSource(req.Context(), m.tier, src); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	// if storing succeeds, just return empty response
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
	handleSuccessfulRequest(w)
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
	if errors.Is(err, aggregate.ErrNotFound) {
		// we don't throw an error, just return empty response
		return
	} else if err != nil && !errors.Is(err, aggregate.ErrNotActive) {
		handleInternalServerError(w, "", err)
		return
	}
	// to send ret back, marshal to json and then write it back
	ser, err := json.Marshal(&ret)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	_, _ = w.Write(ser)
}

func (m server) RunAggregate(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var aggReq struct {
		Name     string `json:"Name"`
		Duration int    `json:"Duration"`
	}
	if err := json.Unmarshal(data, &aggReq); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	// call controller
	if err := aggregate2.RunAggregate(req.Context(), m.tier, ftypes.AggName(aggReq.Name), aggReq.Duration); err != nil {
		handleInternalServerError(w, "", err)
		return
	}
	handleSuccessfulRequest(w)
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
	handleSuccessfulRequest(w)
}

func (m server) DisableConnector(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var connReq struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(data, &connReq); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	err = connector2.DisableConnector(req.Context(), m.tier, connReq.Name)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
}

func (m server) DeleteConnector(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var connReq struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(data, &connReq); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	err = connector2.DeleteConnector(req.Context(), m.tier, connReq.Name)
	if err != nil {
		handleInternalServerError(w, "", err)
		return
	}
}

func (m server) DeleteSource(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var sourceReq struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(data, &sourceReq); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	err = connector2.DeleteSource(req.Context(), m.tier, sourceReq.Name)
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
	_, _ = w.Write(value.ToJSON(ret))
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
	_, _ = w.Write(value.ToJSON(value.NewList(ret...)))
}

func (m server) UploadModel(w http.ResponseWriter, req *http.Request) {
	mr, err := req.MultipartReader()
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	// maxMemory: 1 GB (max memory to use in RAM, remaining data is stored in disk as temporary files)
	form, err := mr.ReadForm(1 << 30)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	formFile, err := getFileFromMultiPartForm(form, "file")
	if err != nil {
		handleBadRequest(w, "", err)
	}
	modelFile, err := formFile.Open()
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	values, err := getValuesFromMultiPartForm(form, []string{"name", "version", "framework", "framework_version"})
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	modelReq := sagemaker.ModelUploadRequest{
		Name:             values["name"],
		Version:          values["version"],
		Framework:        values["framework"],
		FrameworkVersion: values["framework_version"],
		ModelFile:        modelFile,
	}
	err = modelstore.Store(req.Context(), m.tier, modelReq)
	if err != nil {
		var retry modelstore.RetryError
		if errors.As(err, &retry) {
			handleServiceUnavailable(w, "", retry)
		} else {
			handleInternalServerError(w, "", err)
		}
		return
	}
	handleSuccessfulRequest(w)
}

func (m server) DeleteModel(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var delReq struct {
		Name    string
		Version string
	}
	if err := json.Unmarshal(data, &delReq); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	err = modelstore.Remove(req.Context(), m.tier, delReq.Name, delReq.Version)
	if err != nil {
		var retry modelstore.RetryError
		if errors.As(err, &retry) {
			handleServiceUnavailable(w, "", err)
		} else {
			handleInternalServerError(w, "", err)
		}
		return
	}
	handleSuccessfulRequest(w)
}

func (m server) EnableModel(w http.ResponseWriter, req *http.Request) {
	data, err := readRequest(req)
	if err != nil {
		handleBadRequest(w, "", err)
		return
	}
	var modelName struct {
		Model string `json:"Model"`
	}
	if err := json.Unmarshal(data, &modelName); err != nil {
		handleBadRequest(w, "invalid request: ", err)
		return
	}
	err = modelstore.EnableModel(req.Context(), m.tier, modelName.Model)
	if err != nil {
		var retry modelstore.RetryError
		if errors.As(err, &retry) {
			handleServiceUnavailable(w, "", err)
		} else {
			handleInternalServerError(w, "", err)
		}
		return
	}
	handleSuccessfulRequest(w)
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
	_, _ = w.Write(data)
}

// handleSuccessfulRequest explicitly writes `StatusOk` to the ResponseWriter instance.
//
// this should be used for methods which do not write anything back as part of the response body i.e. do not call
// `w.Write()` since that would automatically add `StatusOk` header
func handleSuccessfulRequest(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
}

func handleBadRequest(w http.ResponseWriter, errorPrefix string, err error) {
	http.Error(w, fmt.Sprintf("%s%v", errorPrefix, err), http.StatusBadRequest)
	log.Printf("Error: %v", err)
}

func handleInternalServerError(w http.ResponseWriter, errorPrefix string, err error) {
	http.Error(w, fmt.Sprintf("%s%v", errorPrefix, err), http.StatusInternalServerError)
	log.Printf("Error: %v", err)
}

func handleServiceUnavailable(w http.ResponseWriter, errorPrefix string, err error) {
	http.Error(w, fmt.Sprintf("%s%v", errorPrefix, err), http.StatusServiceUnavailable)
	log.Printf("Error: %v", err)
}

func handleTooManyRequests(w http.ResponseWriter, errorPrefix string, err error) {
	http.Error(w, fmt.Sprintf("%s%v", errorPrefix, err), http.StatusTooManyRequests)
	log.Printf("Error: %v", err)
}

func getValueFromMultiPartForm(form *multipart.Form, key string) (string, error) {
	x, ok := form.Value[key]
	if !ok {
		return "", fmt.Errorf("'%s' not found in form's values", key)
	}
	if len(x) == 0 {
		return "", fmt.Errorf("no values found at key '%s' in form", key)
	}
	return x[0], nil
}

func getValuesFromMultiPartForm(form *multipart.Form, keys []string) (map[string]string, error) {
	res := make(map[string]string, len(keys))
	for _, k := range keys {
		v, err := getValueFromMultiPartForm(form, k)
		if err != nil {
			return nil, err
		}
		res[k] = v
	}
	return res, nil
}

func getFileFromMultiPartForm(form *multipart.Form, key string) (*multipart.FileHeader, error) {
	x, ok := form.File[key]
	if !ok {
		return nil, fmt.Errorf("'%s' not found in form's files", key)
	}
	if len(x) == 0 {
		return nil, fmt.Errorf("no files found at key '%s' in form", key)
	}
	return x[0], nil
}
