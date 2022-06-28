package main

import (
	"encoding/json"
	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fmt"
	"github.com/buger/jsonparser"
	"math"
	"strconv"
	"time"
)

const MICRO_SECONDS_PER_SECOND = 1000000

// Map of keys to if they required on not.
var PROFILE_KEYS = map[string]bool{
	"OType":      true,
	"Oid":        true,
	"Key":        true,
	"Value":      true,
	"UpdateTime": false,
}

var ACTION_KEYS = map[string]bool{
	"ActorID":    true,
	"ActorType":  true,
	"TargetID":   true,
	"TargetType": true,
	"ActionType": true,
	"Timestamp":  false,
	"RequestID":  true,
	"Metadata":   false,
}

func GetProfilesFromRest(data []byte) ([]profilelib.ProfileItem, error) {
	var request []restProfile
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, err
	}
	profiles := make([]profilelib.ProfileItem, len(request))
	for i, r := range request {
		profiles[i] = r.profile
	}
	return profiles, nil
}

func GetActionsFromRest(data []byte) ([]actionlib.Action, error) {
	var request []restAction
	if err := json.Unmarshal(data, &request); err != nil {
		return nil, err
	}
	actions := make([]actionlib.Action, len(request))
	for i, r := range request {
		actions[i] = r.action
	}
	return actions, nil
}

type restAction struct {
	action actionlib.Action
}

func (a *restAction) UnmarshalJSON(data []byte) error {
	var fields struct {
		ActionID   ftypes.IDType     `json:"ActionID"`
		ActorID    json.RawMessage   `json:"ActorID"`
		ActorType  ftypes.OType      `json:"ActorType"`
		TargetID   json.RawMessage   `json:"TargetID"`
		TargetType ftypes.OType      `json:"TargetType"`
		ActionType ftypes.ActionType `json:"ActionType"`
		Timestamp  ftypes.Timestamp  `json:"Timestamp"`
		RequestID  json.RawMessage   `json:"RequestID"`
	}

	err := verifyFields(data, ACTION_KEYS)
	if err != nil {
		return fmt.Errorf("error unmarshalling action json: %v", err)
	}
	err = json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling action json: %v", err)
	}
	a.action.ActionID = fields.ActionID
	a.action.ActorID, err = idToStr(fields.ActorID)
	if err != nil {
		return fmt.Errorf("error unmarshalling action json: %w", err)
	}
	a.action.ActorType = fields.ActorType
	a.action.TargetID, err = idToStr(fields.TargetID)
	if err != nil {
		return fmt.Errorf("error unmarshalling action json: %w", err)
	}
	a.action.TargetType = fields.TargetType
	a.action.ActionType = fields.ActionType
	err = validateTime(int64(fields.Timestamp))
	if err != nil {
		return fmt.Errorf("error unmarshalling action json: %w", err)
	}
	a.action.Timestamp = fields.Timestamp
	requestId, err := idToStr(fields.RequestID)
	if err != nil {
		return fmt.Errorf("error unmarshalling action json: %w", err)
	}
	a.action.RequestID = ftypes.RequestID(requestId)

	vdata, vtype, _, err := jsonparser.Get(data, "Metadata")
	if err != nil {
		a.action.Metadata = value.NewDict(map[string]value.Value{})
		return nil
	}
	a.action.Metadata, err = value.ParseJSON(vdata, vtype)
	if err != nil {
		return fmt.Errorf("error parsing metadata from action json: %v", err)
	}
	return nil
}

type restProfile struct {
	profile profilelib.ProfileItem
}

func (p *restProfile) UnmarshalJSON(data []byte) error {
	var fields struct {
		OType      ftypes.OType    `json:"OType"`
		Oid        json.RawMessage `json:"Oid"`
		Key        string          `json:"Key"`
		UpdateTime uint64          `json:"UpdateTime"`
	}

	err := verifyFields(data, PROFILE_KEYS)
	if err != nil {
		return fmt.Errorf("error unmarshalling profile json: %v", err)
	}
	err = json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling profile json: %v", err)
	}
	p.profile.OType = fields.OType
	p.profile.Oid, err = idToStr(fields.Oid)
	if err != nil {
		return fmt.Errorf("error unmarshalling profile json: %w", err)
	}
	p.profile.Key = fields.Key
	err = validateTime(int64(fields.UpdateTime))
	if err != nil {
		return fmt.Errorf("error unmarshalling profile json: %w", err)
	}
	// Profile time is in microseconds.
	p.profile.UpdateTime = fields.UpdateTime * MICRO_SECONDS_PER_SECOND
	vdata, vtype, _, err := jsonparser.Get(data, "Value")
	if err != nil {
		return fmt.Errorf("error getting value from profile json: %v", err)
	}
	p.profile.Value, err = value.ParseJSON(vdata, vtype)
	if err != nil {
		return fmt.Errorf("error parsing value from profile json: %v", err)
	}
	return nil
}

func verifyFields(data []byte, keys map[string]bool) error {
	var mp map[string]interface{}
	err := json.Unmarshal(data, &mp)
	if err != nil {
		return fmt.Errorf("error unmarshalling json: %v", err)
	}
	for key, required := range keys {
		if _, ok := mp[key]; !ok && required {
			return fmt.Errorf("json is missing key: %v", key)
		}
	}
	for k := range mp {
		if _, ok := keys[k]; !ok {
			return fmt.Errorf("json has extra key: %v", k)
		}
	}
	return nil
}

func validateTime(t int64) error {
	if t == 0 {
		return nil
	}
	tm := time.Unix(t, 0)
	if tm.Before(time.Now().Add(-365*24*time.Hour)) || tm.After(time.Now().Add(365*24*time.Hour)) {
		return fmt.Errorf("timestamp field is expected to be in seconds since epoch")
	}
	return nil
}
func idToStr(val json.RawMessage) (ftypes.OidType, error) {
	var v interface{}
	_ = json.Unmarshal(val, &v)
	switch v.(type) {
	case string:
		return ftypes.OidType("\"" + v.(string) + "\""), nil
	case float64:
		f := v.(float64)
		if f != math.Trunc(f) {
			return "", fmt.Errorf("id should be string or int: %v", v)
		}
		return ftypes.OidType(strconv.FormatInt(int64(f), 10)), nil
	case int64:
		return ftypes.OidType(strconv.FormatInt(v.(int64), 10)), nil
	default:
		return "", fmt.Errorf("id should be string or int: %v", v)
	}
}
