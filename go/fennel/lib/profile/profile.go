package profile

import (
	"encoding/json"
	"fmt"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/sql"
	"fennel/lib/value"

	"github.com/buger/jsonparser"
)

const (
	PROFILELOG_KAFKA_TOPIC  = "profilelog"
	MICRO_SECOND_MULTIPLIER = 1000000
)

type ProfileItemKey struct {
	OType ftypes.OType   `db:"otype" json:"OType"`
	Oid   ftypes.OidType `db:"oid" json:"Oid"`
	Key   string         `db:"zkey" json:"Key"`
}

func NewProfileItemKey(otype ftypes.OType, oid ftypes.OidType, k string) ProfileItemKey {
	return ProfileItemKey{
		otype, oid, k,
	}
}

type ProfileItem struct {
	OType      ftypes.OType   `json:"OType"`
	Oid        ftypes.OidType `json:"Oid"`
	Key        string         `json:"Key"`
	Value      value.Value    `json:"Value"`
	UpdateTime uint64         `json:"UpdateTime"`
}

func (pi *ProfileItem) GetProfileKey() ProfileItemKey {
	return ProfileItemKey{
		OType: pi.OType,
		Oid:   pi.Oid,
		Key:   pi.Key,
	}
}

func NewProfileItem(otype ftypes.OType, oid ftypes.OidType, k string, v value.Value, updateTime uint64) ProfileItem {
	return ProfileItem{
		ftypes.OType(otype), oid, k, v, updateTime,
	}
}

func (pi *ProfileItemKey) Validate() error {
	if len(pi.OType) == 0 {
		return fmt.Errorf("otype can not be empty")
	}
	if len(pi.Oid) == 0 {
		return fmt.Errorf("oid can not be zero")
	}
	if len(pi.Key) == 0 {
		return fmt.Errorf("key can not be empty")
	}
	return nil
}

// Validate validates the profile item
func (pi *ProfileItem) Validate() error {
	pk := pi.GetProfileKey()
	return pk.Validate()
}

func (pi *ProfileItem) Equals(other *ProfileItem) bool {
	if pi.OType != other.OType {
		return false
	}
	if pi.Oid != other.Oid {
		return false
	}
	if pi.Key != other.Key {
		return false
	}
	if pi.Value == value.Nil {
		if other.Value != value.Nil {
			return false
		}
	} else if !pi.Value.Equal(other.Value) {
		return false
	}
	if pi.UpdateTime != other.UpdateTime {
		return false
	}
	return true
}

func (pi ProfileItem) MarshalJSON() ([]byte, error) {
	type ProfileItem_ ProfileItem
	pi_ := ProfileItem_(pi)
	pi_.Value = value.Clean(pi.Value)
	return json.Marshal(pi_)
}

func (pi *ProfileItem) UnmarshalJSON(data []byte) error {
	var fields struct {
		OType      ftypes.OType   `json:"OType"`
		Oid        ftypes.OidType `json:"Oid"`
		Key        string         `json:"Key"`
		UpdateTime uint64         `json:"UpdateTime"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling profile json: %v", err)
	}
	pi.OType = fields.OType
	pi.Oid = fields.Oid
	pi.Key = fields.Key
	pi.UpdateTime = fields.UpdateTime
	vdata, vtype, _, err := jsonparser.Get(data, "Value")
	if err != nil {
		return fmt.Errorf("error getting value from profile json: %v", err)
	}
	pi.Value, err = value.ParseJSON(vdata, vtype)
	if err != nil {
		return fmt.Errorf("error parsing value from profile json: %v", err)
	}
	return nil
}

func (pi ProfileItem) ToValueDict() (value.Dict, error) {
	if pi.UpdateTime == 0 {
		pi.UpdateTime = uint64(time.Now().Unix())
	} else if pi.UpdateTime > uint64(time.Now().Unix())+uint64(time.Hour.Seconds()) {
		//  Convert microseconds to seconds
		pi.UpdateTime = pi.UpdateTime / 1000000
	}

	oid, err := value.FromJSON([]byte(pi.Oid))
	if err != nil {
		return value.Dict{}, err
	}
	return value.NewDict(map[string]value.Value{
		"oid":       oid,
		"otype":     value.String(pi.OType),
		"key":       value.String(pi.Key),
		"timestamp": value.Int(pi.UpdateTime),
		"value":     pi.Value,
	}), nil
}

func FromValueDict(dict value.Dict) (ProfileItem, error) {
	var pi ProfileItem
	if oid, ok := dict.Get("oid"); ok {
		pi.Oid = ftypes.OidType(value.ToJSON(oid))
	} else {
		return pi, fmt.Errorf("oid not found in profile dict")
	}

	if otype, ok := dict.Get("otype"); ok {
		if o, ok := otype.(value.String); ok {
			pi.OType = ftypes.OType(o)
		} else {
			return pi, fmt.Errorf("otype not a string")
		}
	} else {
		return pi, fmt.Errorf("otype not found in profile dict")
	}

	if key, ok := dict.Get("key"); ok {
		if k, ok := key.(value.String); ok {
			pi.Key = string(k)
		} else {
			return pi, fmt.Errorf("key not a string")
		}
	} else {
		return pi, fmt.Errorf("key not found in profile dict")
	}

	if timestamp, ok := dict.Get("timestamp"); ok {
		if t, ok := timestamp.(value.Int); ok {
			pi.UpdateTime = uint64(t) * MICRO_SECOND_MULTIPLIER
		} else {
			return pi, fmt.Errorf("timestamp not an int")
		}
	} else {
		pi.UpdateTime = uint64(time.Now().UnixMicro())
	}

	if value, ok := dict.Get("value"); ok {
		pi.Value = value
	} else {
		return pi, fmt.Errorf("value not found in profile dict")
	}

	return pi, nil
}

func ToJsonList(items []ProfileItem) ([][]byte, error) {
	res := make([][]byte, len(items))
	for i, item := range items {
		data, err := item.MarshalJSON()
		if err != nil {
			return nil, err
		}
		res[i] = data
	}
	return res, nil
}

// ToList takes a list of profiles and arranges that in a value.List
// else returns errors
func ToList(profiles []ProfileItem) (value.List, error) {
	table := value.List{}
	table.Grow(len(profiles))
	for i := range profiles {
		d, err := profiles[i].ToValueDict()
		if err != nil {
			return value.List{}, err
		}
		table.Append(d)
	}
	return table, nil
}

type QueryRequest struct {
	Otype ftypes.OType   `json:"otype"`
	Oid   ftypes.OidType `json:"oid"`
	sql.Pagination
}
