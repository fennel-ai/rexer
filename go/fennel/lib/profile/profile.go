package profile

import (
	"encoding/json"
	"fmt"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/buger/jsonparser"
)

const (
	PROFILELOG_KAFKA_TOPIC = "profilelog"
)

type ProfileItemKey struct {
	OType ftypes.OType `db:"otype" json:"OType"`
	Oid   string       `db:"oid" json:"Oid"`
	Key   string       `db:"zkey" json:"Key"`
}

func NewProfileItemKey(otype string, oid string, k string) ProfileItemKey {
	return ProfileItemKey{
		ftypes.OType(otype), oid, k,
	}
}

type ProfileItem struct {
	OType      ftypes.OType `json:"OType"`
	Oid        string       `json:"Oid"`
	Key        string       `json:"Key"`
	Value      value.Value  `json:"Value"`
	UpdateTime uint64       `json:"UpdateTime"`
}

func (pi *ProfileItem) GetProfileKey() ProfileItemKey {
	return ProfileItemKey{
		OType: pi.OType,
		Oid:   pi.Oid,
		Key:   pi.Key,
	}
}

func NewProfileItem(otype string, oid string, k string, v value.Value, updateTime uint64) ProfileItem {
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
		OType      ftypes.OType `json:"OType"`
		Oid        string       `json:"Oid"`
		Key        string       `json:"Key"`
		UpdateTime uint64       `json:"UpdateTime"`
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

func (pi ProfileItem) ToValueDict() (*value.Dict, error) {
	if pi.UpdateTime == 0 {
		pi.UpdateTime = uint64(time.Now().Unix())
	} else if pi.UpdateTime > uint64(time.Now().Unix())+uint64(time.Hour.Seconds()) {
		//  Convert microseconds to seconds
		pi.UpdateTime = uint64(pi.UpdateTime / 1000000)
	}

	oid, err := value.FromJSON([]byte(pi.Oid))
	if err != nil {
		return nil, err
	}
	return value.NewDict(map[string]value.Value{
		"oid":       oid,
		"otype":     value.String(pi.OType),
		"key":       value.String(pi.Key),
		"timestamp": value.Int(pi.UpdateTime),
		"value":     pi.Value,
	}), nil
}

// ToList takes a list of profiles and arranges that in a value.List
// else returns errors
func ToList(profiles []ProfileItem) (value.List, error) {
	table := value.List{}
	for i := range profiles {
		d, err := profiles[i].ToValueDict()
		if err != nil {
			return value.List{}, err
		}
		table.Append(d)
	}
	return table, nil
}
