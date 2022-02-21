package main

import (
	"bytes"
	"encoding/json"
	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

const goUrl = "http://localhost:2425"
const (
	pathGetProfile      = goUrl + "/get"
	pathGetProfileMulti = goUrl + "/get_multi"
	pathFetchActions    = goUrl + "/fetch"
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
		return strconv.ParseUint(vals.Get(key), 10, 64)
	} else if !optional {
		return 0, fmt.Errorf("missing required argument '%s'", key)
	}
	return 0, nil
}

func loadProfileQueryValues(vals url.Values, otype *string, oid *uint64, key *string, version *uint64) error {
	var err error
	if *otype, err = getString(vals, "otype", false); err != nil {
		return err
	}
	if *oid, err = getUint64(vals, "oid", false); err != nil {
		return err
	}
	if *key, err = getString(vals, "key", false); err != nil {
		return err
	}
	if *version, err = getUint64(vals, "version", true); err != nil {
		return err
	}
	return nil
}

func ProfileHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// Process the request
		var otype, key string
		var oid, version uint64
		err := loadProfileQueryValues(req.URL.Query(), &otype, &oid, &key, &version)
		if err != nil {
			handleInvalidRequest(w, err)
			return
		}
		p := profile.NewProfileItem(otype, oid, key, version)
		if err := p.Validate(); err != nil {
			handleInvalidRequest(w, err)
			return
		}
		// Call the server and write back the response
		ser, err := json.Marshal(p)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		response, err := postJSON(ser, pathGetProfile)
		fmt.Printf("profile response: %v\n", response)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		w.Write(response)
	default:
		http.Error(w, "unsupported request method", http.StatusMethodNotAllowed)
	}
}

func loadProfileMultiQueryValues(vals url.Values, otype *string, oid *uint64, key *string, version *uint64) error {
	var err error
	if *otype, err = getString(vals, "otype", true); err != nil {
		return err
	}
	if *oid, err = getUint64(vals, "oid", true); err != nil {
		return err
	}
	if *key, err = getString(vals, "key", true); err != nil {
		return err
	}
	if *version, err = getUint64(vals, "version", true); err != nil {
		return err
	}
	return nil
}

func ProfileMultiHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// Process the request
		var otype, key string
		var oid, version uint64
		err := loadProfileMultiQueryValues(req.URL.Query(), &otype, &oid, &key, &version)
		if err != nil {
			handleInvalidRequest(w, err)
			return
		}
		pfr := profile.ProfileFetchRequest{OType: ftypes.OType(otype), Oid: oid, Key: key, Version: version}
		// Call the server and write back the response
		ser, err := json.Marshal(pfr)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		response, err := postJSON(ser, pathGetProfileMulti)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		w.Write(response)
	default:
		http.Error(w, "unsupported request method", http.StatusMethodNotAllowed)
	}
}

func loadActionsQueryValues(vals url.Values, actorID *uint64, actorType *string, targetID *uint64, targetType *string,
	actionType *string, minTimestamp *uint64, maxTimestamp *uint64, minActionID *uint64, maxActionID *uint64) error {
	var err error
	if *actorID, err = getUint64(vals, "actor_id", true); err != nil {
		return err
	}
	if *actorType, err = getString(vals, "actor_type", true); err != nil {
		return err
	}
	if *targetID, err = getUint64(vals, "target_id", true); err != nil {
		return err
	}
	if *targetType, err = getString(vals, "targetr_type", true); err != nil {
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
	return nil
}

func ActionsHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// Process the request
		var actorType, targetType, actionType string
		var actorID, targetID, minTimestamp, maxTimestamp, minActionID, maxActionID uint64
		err := loadActionsQueryValues(req.URL.Query(), &actorID, &actorType, &targetID, &targetType, &actionType,
			&minTimestamp, &maxTimestamp, &minActionID, &maxActionID)
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
			MinActionID:  ftypes.OidType(maxActionID),
			MaxActionID:  ftypes.OidType(maxActionID),
		}
		// Call the server and write back the response
		ser, err := json.Marshal(afr)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		response, err := postJSON(ser, pathFetchActions)
		if err != nil {
			handleInternalServerError(w, err)
			return
		}
		w.Write(response)
	default:
		http.Error(w, "unsupported request method", http.StatusMethodNotAllowed)
	}
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/profile/", ProfileHandler)
	router.HandleFunc("/actions/", ActionsHandler)
	router.HandleFunc("/profile_multi/", ProfileMultiHandler)
	log.Println("starting http service on :2475")
	log.Fatal(http.ListenAndServe(":2475", router))
}
