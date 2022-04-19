package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/profile"

	"github.com/gorilla/mux"
)

const (
	pathGetProfile      = "/get"
	pathGetProfileMulti = "/get_multi"
	pathFetchActions    = "/fetch"
)

func postJSON(data []byte, url string) ([]byte, error) {
	reqBody := bytes.NewBuffer(data)
	response, err := http.Post(url, "application/json", reqBody)
	if err != nil {
		return nil, fmt.Errorf("server error: %v", err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read server response: %v", err)
	}
	// handle http error given by the server
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", http.StatusText(response.StatusCode), string(body))
	}
	return body, nil
}

func handleInvalidRequest(w http.ResponseWriter, err error) {
	http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
	log.Printf("Error: %v", err)
}

func handleInternalServerError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
	log.Printf("Error: %v", err)
}

func getString(vals url.Values, key string, optional bool) (string, error) {
	if vals.Has(key) {
		return vals.Get(key), nil
	} else if !optional {
		return "", fmt.Errorf("missing required argument '%s'", key)
	}
	return "", nil
}

func getUint64(vals url.Values, key string, optional bool) (uint64, error) {
	if vals.Has(key) {
		res, err := strconv.ParseUint(vals.Get(key), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unable to parse '%s' into an unsigned 64-bit integer", vals.Get(key))
		}
		return res, nil
	} else if !optional {
		return 0, fmt.Errorf("missing required argument '%s'", key)
	}
	return 0, nil
}

func loadProfileQueryValues(vals url.Values, otype *string, oid *string, key *string) error {
	var err error
	if *otype, err = getString(vals, "otype", false); err != nil {
		return err
	}
	if *oid, err = getString(vals, "oid", false); err != nil {
		return err
	}
	if *key, err = getString(vals, "key", false); err != nil {
		return err
	}

	return nil
}

func (s server) ProfileHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// Process the request
		var otype, oid, key string
		err := loadProfileQueryValues(req.URL.Query(), &otype, &oid, &key)
		if err != nil {
			handleInvalidRequest(w, err)
			return
		}
		pk := profile.NewProfileItemKey(otype, oid, key)
		if err := pk.Validate(); err != nil {
			handleInvalidRequest(w, err)
			return
		}
		// Call the server and write back the response
		ser, err := json.Marshal(pk)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		response, err := postJSON(ser, s.endpoint+pathGetProfile)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		w.Write(response)
	default:
		http.Error(w, "unsupported request method", http.StatusMethodNotAllowed)
	}
}

func loadProfileMultiQueryValues(vals url.Values, otype *string, oid *string, key *string, updateTime *uint64) error {
	var err error
	if *otype, err = getString(vals, "otype", true); err != nil {
		return err
	}
	if *oid, err = getString(vals, "oid", true); err != nil {
		return err
	}
	if *key, err = getString(vals, "key", true); err != nil {
		return err
	}
	if *updateTime, err = getUint64(vals, "updateTime", true); err != nil {
		return err
	}
	return nil
}

func (s server) ProfileMultiHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// Process the request
		var otype, oid, key string
		var updateTime uint64
		err := loadProfileMultiQueryValues(req.URL.Query(), &otype, &oid, &key, &updateTime)
		if err != nil {
			handleInvalidRequest(w, err)
			return
		}
		pfr := profile.ProfileItemKey{OType: ftypes.OType(otype), Oid: oid, Key: key}
		// Call the server and write back the response
		pk, err := json.Marshal(pfr)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		response, err := postJSON(pk, s.endpoint+pathGetProfileMulti)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		w.Write(response)
	default:
		http.Error(w, "unsupported request method", http.StatusMethodNotAllowed)
	}
}

func loadActionsQueryValues(
	vals url.Values, actorID *string, actorType *string, targetID *string, targetType *string, actionType *string,
	minTimestamp *uint64, maxTimestamp *uint64, minActionID *uint64, maxActionID *uint64, requestID *uint64) error {
	var err error
	if *actorID, err = getString(vals, "actor_id", true); err != nil {
		return err
	}
	if *actorType, err = getString(vals, "actor_type", true); err != nil {
		return err
	}
	if *targetID, err = getString(vals, "target_id", true); err != nil {
		return err
	}
	if *targetType, err = getString(vals, "target_type", true); err != nil {
		return err
	}
	if *actionType, err = getString(vals, "action_type", true); err != nil {
		return err
	}
	if *minTimestamp, err = getUint64(vals, "min_timestamp", true); err != nil {
		return err
	}
	if *maxTimestamp, err = getUint64(vals, "max_timestamp", true); err != nil {
		return err
	}
	if *minActionID, err = getUint64(vals, "min_action_id", true); err != nil {
		return err
	}
	if *maxActionID, err = getUint64(vals, "max_action_id", true); err != nil {
		return err
	}
	if *requestID, err = getUint64(vals, "request_id", true); err != nil {
		return err
	}
	return nil
}

func (s server) ActionsHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// Process the request
		var actorID, actorType, targetID, targetType, actionType string
		var minTimestamp, maxTimestamp, minActionID, maxActionID, requestID uint64
		err := loadActionsQueryValues(req.URL.Query(), &actorID, &actorType, &targetID, &targetType, &actionType,
			&minTimestamp, &maxTimestamp, &minActionID, &maxActionID, &requestID)
		if err != nil {
			handleInvalidRequest(w, err)
			return
		}
		afr := action.ActionFetchRequest{
			ActorID:      ftypes.OidType(actorID),
			ActorType:    ftypes.OType(actorType),
			TargetID:     ftypes.OidType(targetID),
			TargetType:   ftypes.OType(targetType),
			ActionType:   ftypes.ActionType(actionType),
			MinTimestamp: ftypes.Timestamp(minTimestamp),
			MaxTimestamp: ftypes.Timestamp(maxTimestamp),
			MinActionID:  ftypes.IDType(minActionID),
			MaxActionID:  ftypes.IDType(maxActionID),
			RequestID:    ftypes.RequestID(requestID),
		}
		// Call the server and write back the response
		ser, err := json.Marshal(afr)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		response, err := postJSON(ser, s.endpoint+pathFetchActions)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		w.Write(response)
	default:
		http.Error(w, "unsupported request method", http.StatusMethodNotAllowed)
	}
}

func setRoutes(s *server) {
	s.router = mux.NewRouter()
	s.router.HandleFunc("/profile/", s.ProfileHandler)
	s.router.HandleFunc("/actions/", s.ActionsHandler)
	s.router.HandleFunc("/profile_multi/", s.ProfileMultiHandler)
}

func createServer(port uint32, endpoint string) *server {
	server := server{
		port:     port,
		endpoint: endpoint,
	}
	setRoutes(&server)
	return &server
}

type server struct {
	port     uint32
	endpoint string
	router   *mux.Router
}

func (s server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
