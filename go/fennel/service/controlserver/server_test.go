package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"fennel/lib/action"
	"fennel/lib/profile"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestServer_ProfileHandler(t *testing.T) {
	// Prepare only valid request that will be sent
	reqStr := fmt.Sprintf("/profile/?otype=%s&oid=%d&key=%s&version=%d",
		"abc", uint64(math.MaxUint64), "xyz", uint64(math.MaxUint64-1))
	req := httptest.NewRequest("GET", reqStr, nil)
	// Prepare the expected ProfileItem
	expected := profile.NewProfileItem("abc", math.MaxUint64, "xyz", math.MaxUint64-1)
	// Prepare value that will be returned by the server
	val := value.Double(3.14)
	valSer, err := json.Marshal(val)
	assert.NoError(t, err)
	// Set up the endpoint server
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read request
		data, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		r.Body.Close()
		// Verify request unmarshals properly into a ProfileItem
		var pi profile.ProfileItem
		err = json.Unmarshal(data, &pi)
		assert.NoError(t, err)
		assert.True(t, expected.Equals(&pi))
		// Write back prepared value
		w.Write(valSer)
	}))
	defer es.Close()
	// Set up server and test valid request
	s := createServer("", es.URL)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	// Verify response is as expected
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, string(valSer), rr.Body.String())
	// Now test for invalid requests
	badReqStrs := []string{
		"",
		"oid=1&key=abc",
		"otype=&oid=1&key=abc",
		"otype=type1&key=abc",
		"otype=type1&oid=&key=abc",
		"otype=type1&oid=1",
		"otype=type1&oid=1&key=",
		"otype=type1&oid=-1&key=abc",
		"otype=type1&oid=abc&key=abc",
		"otype=type1&oid=1&key=abc&version=-2",
		"otype=type1&oid=1&key=abc&version=abc",
	}
	for _, badStr := range badReqStrs {
		req := httptest.NewRequest("GET", "/profile/?"+badStr, nil)
		rr := httptest.NewRecorder()
		s.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	}
}

func TestServer_ProfileMultiHandler(t *testing.T) {
	// Prepare only valid request that will be sent
	reqStr := fmt.Sprintf("/profile_multi/?otype=%s&oid=%d&key=%s&version=%d",
		"abc", uint64(math.MaxUint64), "xyz", uint64(math.MaxUint64-1))
	req := httptest.NewRequest("GET", reqStr, nil)
	// Prepare the expected ProfileFetchRequest
	expected := profile.ProfileFetchRequest{OType: "abc", Oid: math.MaxUint64, Key: "xyz", Version: math.MaxUint64 - 1}
	// Prepare profiles that will be returned by the server
	profiles := make([]profile.ProfileItem, 0)
	profiles = append(profiles, profile.ProfileItem{OType: "1", Oid: math.MaxUint64 - 2, Key: "3",
		Version: math.MaxUint64 - 4, Value: value.Int(5)})
	profiles = append(profiles, profile.ProfileItem{OType: "5", Oid: 4, Key: "3", Version: 2, Value: value.Int(1)})
	profilesSer, err := json.Marshal(profiles)
	assert.NoError(t, err)
	// Set up the endpoint server
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read request
		data, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		r.Body.Close()
		// Verify request unmarshals properly into a ProfileFetchRequest
		var pfr profile.ProfileFetchRequest
		err = json.Unmarshal(data, &pfr)
		assert.NoError(t, err)
		assert.Equal(t, expected, pfr)
		// Write back prepared list of profiles
		w.Write(profilesSer)
	}))
	defer es.Close()
	// Set up server and test valid request
	s := createServer("", es.URL)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	// Verify response is as expected
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, string(profilesSer), rr.Body.String())
	// Now test for invalid requests
	badReqStrs := []string{
		"oid=-1",
		"oid=abc",
		"version=-2",
		"version=abc",
	}
	for _, badStr := range badReqStrs {
		req = httptest.NewRequest("GET", "/profile_multi/?"+badStr, nil)
		rr := httptest.NewRecorder()
		s.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	}
}

func TestServer_ActionsHandler(t *testing.T) {
	var max uint64 = math.MaxUint64
	// Prepare only valid request that will be sent
	reqStr := fmt.Sprintf("/actions/?actor_id=%d&actor_type=%s&target_id=%d&target_type=%s&action_type=%s&"+
		"request_id=%d&min_timestamp=%d&max_timestamp=%d&min_action_id=%d&max_action_id=%d",
		max, "a", max-1, "b", "c", max-2, max-3, max-4, max-5, max-6)
	req := httptest.NewRequest("GET", reqStr, nil)
	// Prepare the expected ActionFetchRequest
	expected := action.ActionFetchRequest{
		ActorID:      math.MaxUint64,
		ActorType:    "a",
		TargetID:     math.MaxUint64 - 1,
		TargetType:   "b",
		ActionType:   "c",
		RequestID:    math.MaxUint64 - 2,
		MinTimestamp: math.MaxUint64 - 3,
		MaxTimestamp: math.MaxUint64 - 4,
		MinActionID:  math.MaxUint64 - 5,
		MaxActionID:  math.MaxUint64 - 6,
	}
	// Prepare actions that will be returned by the server
	actions := make([]action.Action, 0)
	actions = append(actions, action.Action{ActionID: math.MaxUint64 - 1, ActorID: math.MaxUint64 - 2, ActorType: "3",
		TargetID: math.MaxUint64 - 4, TargetType: "5", ActionType: "6", Timestamp: math.MaxUint64 - 7,
		RequestID: math.MaxUint64 - 8, Metadata: value.Int(9)})
	actions = append(actions, action.Action{ActionID: 8, ActorID: 7, ActorType: "6", TargetID: 5, TargetType: "4",
		ActionType: "3", Timestamp: 2, RequestID: 1, Metadata: value.String("abc")})
	actionsSer, err := json.Marshal(actions)
	assert.NoError(t, err)
	// Set up the endpoint server
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read request
		data, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		r.Body.Close()
		// Verify request unmarshals properly into an ActionFetchRequest
		var afr action.ActionFetchRequest
		err = json.Unmarshal(data, &afr)
		assert.NoError(t, err)
		assert.Equal(t, expected, afr)
		// Write back prepared list of actions
		w.Write(actionsSer)
	}))
	defer es.Close()
	// Set up server and test valid request
	s := createServer("", es.URL)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	// Verify response is as expected
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, string(actionsSer), rr.Body.String())
	// Now test for invalid requests
	badReqStrs := []string{
		"actor_id=-1",
		"actor_id=abc",
		"target_id=-1",
		"target_id=abc",
		"request_id=-1",
		"request_id=abc",
		"min_timestamp=-1",
		"min_timestamp=abc",
		"max_timestamp=-1",
		"max_timestamp=abc",
		"min_action_id=-1",
		"min_action_id=abc",
		"max_action_id=-1",
		"max_action_id=abc",
	}
	for _, badStr := range badReqStrs {
		req = httptest.NewRequest("GET", "/actions/?"+badStr, nil)
		rr := httptest.NewRecorder()
		s.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	}
}
