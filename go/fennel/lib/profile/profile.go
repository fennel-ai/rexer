package profile

import (
	"encoding/json"
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/buger/jsonparser"
)

const (
	PROFILELOG_KAFKA_TOPIC = "profilelog"
)

type ProfileItemKey struct {
	OType ftypes.OType `db:"otype" json:"OType"`
	Oid   uint64       `db:"oid" json:"Oid"`
	Key   string       `db:"zkey" json:"Key"`
}

func NewProfileItemKey(otype string, oid uint64, k string) ProfileItemKey {
	return ProfileItemKey{
		ftypes.OType(otype), oid, k,
	}
}

type ProfileItem struct {
	OType      ftypes.OType `json:"OType"`
	Oid        uint64       `json:"Oid"`
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

func NewProfileItem(otype string, oid uint64, k string, v value.Value, updateTime uint64) ProfileItem {
	return ProfileItem{
		ftypes.OType(otype), oid, k, v, updateTime,
	}
}

// func NewProfileItemSer(otype string, oid uint64, key string, version uint64, val []byte) ProfileItemSer {
// 	return ProfileItemSer{
// 		ftypes.OType(otype), oid, key, version, val,
// 	}
// }

// // ToProfileItem converts a ProfileItemSer to ProfileItem
// func (ser *ProfileItemSer) ToProfileItem() (*ProfileItem, error) {
// 	pr := ProfileItem{ser.OType, ser.Oid, ser.Key, ser.UpdateTime, value.Nil}
// 	val, err := value.FromJSON(ser.Value)
// 	if err != nil {
// 		return nil, err
// 	}
// 	pr.Value = val
// 	return &pr, nil
// }

// func FromProfileItemSerList(plSer []ProfileItemSer) ([]ProfileItem, error) {
// 	pl := make([]ProfileItem, len(plSer))
// 	for i, prSer := range plSer {
// 		pr, err := prSer.ToProfileItem()
// 		if err != nil {
// 			return nil, err
// 		}
// 		pl[i] = *pr
// 	}
// 	return pl, nil
// }

// type ProfileItemKey struct {
// 	OType      ftypes.OType `db:"otype" json:"OType"`
// 	Oid        uint64       `db:"oid" json:"Oid"`
// 	Key        string       `db:"zkey" json:"Key"`
// 	Value      []byte       `db:"value" json:"Value"`
// 	UpdateTime uint64       `db:"version" json:"Version"`
// }

func (pi *ProfileItemKey) Validate() error {
	if len(pi.OType) == 0 {
		return fmt.Errorf("otype can not be empty")
	}
	if pi.Oid == 0 {
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
	if pi.Value == nil {
		if other.Value != nil {
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
		Oid        uint64       `json:"Oid"`
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
