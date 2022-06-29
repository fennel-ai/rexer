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

const MicroSecondsPerSecond = 1000000

// ProfileKeys map keys to if they required on not.
var ProfileKeys = map[string]bool{
	"otype":      true,
	"oid":        true,
	"key":        true,
	"value":      true,
	"updateTime": false,
}

var ActionKeys = map[string]bool{
	"actorId":    true,
	"actorType":  true,
	"targetId":   true,
	"targetType": true,
	"actionType": true,
	"timestamp":  false,
	"requestId":  true,
	"metadata":   false,
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
		ActionID   ftypes.IDType     `json:"actionId"`
		ActorID    json.RawMessage   `json:"actorId"`
		ActorType  ftypes.OType      `json:"actorType"`
		TargetID   json.RawMessage   `json:"targetId"`
		TargetType ftypes.OType      `json:"targetType"`
		ActionType ftypes.ActionType `json:"actionType"`
		Timestamp  float64           `json:"timestamp"`
		RequestID  json.RawMessage   `json:"requestId"`
	}

	err := verifyFields(data, ActionKeys)
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
	a.action.Timestamp = ftypes.Timestamp(fields.Timestamp)
	requestId, err := idToStr(fields.RequestID)
	if err != nil {
		return fmt.Errorf("error unmarshalling action json: %w", err)
	}
	a.action.RequestID = ftypes.RequestID(requestId)

	vdata, vtype, _, err := jsonparser.Get(data, "metadata")
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
		OType      ftypes.OType    `json:"otype"`
		Oid        json.RawMessage `json:"oid"`
		Key        string          `json:"key"`
		UpdateTime float64         `json:"updateTime"`
	}

	err := verifyFields(data, ProfileKeys)
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
	p.profile.UpdateTime = uint64(fields.UpdateTime * MicroSecondsPerSecond)
	vdata, vtype, _, err := jsonparser.Get(data, "value")
	if err != nil {
		return fmt.Errorf("error getting value from profile json: %v", err)
	}
	p.profile.Value, err = value.ParseJSON(vdata, vtype)
	fmt.Println("Recieved profile: ", p.profile)
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
