package profile

import (
	"encoding/json"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"github.com/buger/jsonparser"
	"google.golang.org/protobuf/proto"
)

type ProfileItem struct {
	OType   ftypes.OType `db:"otype"`
	Oid     uint64       `db:"oid"`
	Key     string       `db:"zkey"`
	Version uint64       `db:"version"`
	Value   value.Value  `db:"value"`
}

func NewProfileItem(otype string, oid uint64, k string, version uint64) ProfileItem {
	return ProfileItem{
		ftypes.OType(otype), oid, k, version, value.Nil,
	}
}

func NewProfileItemSer(otype string, oid uint64, key string, version uint64, val []byte) ProfileItemSer {
	return ProfileItemSer{
		ftypes.OType(otype), oid, key, version, val,
	}
}

func FromProtoProfileItem(ppr *ProtoProfileItem) (ProfileItem, error) {
	v, err := value.FromProtoValue(ppr.Value)
	if err != nil {
		return ProfileItem{}, err
	}
	return ProfileItem{
		ftypes.OType(ppr.OType),
		ppr.Oid,
		ppr.Key,
		ppr.Version,
		v,
	}, nil
}
func ToProtoProfileItem(pi *ProfileItem) (ProtoProfileItem, error) {
	pv, err := value.ToProtoValue(pi.Value)
	if err != nil {
		return ProtoProfileItem{}, err
	}
	return ProtoProfileItem{
		OType:   string(pi.OType),
		Oid:     pi.Oid,
		Key:     pi.Key,
		Version: pi.Version,
		Value:   &pv,
	}, nil
}

type ProfileItemSer struct {
	OType   ftypes.OType `db:"otype"`
	Oid     uint64       `db:"oid"`
	Key     string       `db:"zkey"`
	Version uint64       `db:"version"`
	Value   []byte       `db:"value"`
}

// Converts a ProfileItemSer to ProfileItem
func (ser *ProfileItemSer) ToProfileItem() (*ProfileItem, error) {
	pr := ProfileItem{ser.OType, ser.Oid, ser.Key, ser.Version, value.Nil}

	var pval value.PValue
	if err := proto.Unmarshal(ser.Value, &pval); err != nil {
		return nil, err
	}

	val, err := value.FromProtoValue(&pval)
	if err != nil {
		return nil, err
	}

	pr.Value = val
	return &pr, nil
}

func FromProfileItemSerList(pl_ser []ProfileItemSer) ([]ProfileItem, error) {
	pl := []ProfileItem{}
	for _, pr_ser := range pl_ser {
		pr, err := pr_ser.ToProfileItem()
		if err != nil {
			return nil, err
		}
		pl = append(pl, *pr)
	}
	return pl, nil
}

type ProfileFetchRequest struct {
	OType   ftypes.OType `db:"otype" json:"OType"`
	Oid     uint64       `db:"oid" json:"Oid"`
	Key     string       `db:"zkey" json:"Key"`
	Version uint64       `db:"version" json:"Version"`
}

func FromProtoProfileFetchRequest(ppfr *ProtoProfileFetchRequest) ProfileFetchRequest {
	return ProfileFetchRequest{
		ftypes.OType(ppfr.OType),
		ppfr.Oid,
		ppfr.Key,
		ppfr.Version,
	}
}

func ToProtoProfileFetchRequest(pfr *ProfileFetchRequest) ProtoProfileFetchRequest {
	return ProtoProfileFetchRequest{
		OType:   string(pfr.OType),
		Oid:     pfr.Oid,
		Key:     pfr.Key,
		Version: pfr.Version,
	}
}

func FromProtoProfileList(profileList *ProtoProfileList) ([]ProfileItem, error) {
	profiles := make([]ProfileItem, len(profileList.Profiles))
	for i, ppr := range profileList.Profiles {
		var err error
		profiles[i], err = FromProtoProfileItem(ppr)
		if err != nil {
			return nil, err
		}
	}
	return profiles, nil
}

func ToProtoProfileList(profiles []ProfileItem) (*ProtoProfileList, error) {
	ret := &ProtoProfileList{}
	ret.Profiles = make([]*ProtoProfileItem, len(profiles))
	for i, profile := range profiles {
		ppr, err := ToProtoProfileItem(&profile)
		if err != nil {
			return nil, err
		}
		ret.Profiles[i] = &ppr
	}
	return ret, nil
}

// Validate validates the profile item
func (pi *ProfileItem) Validate() error {
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

func (pi *ProfileItem) Equals(pi2 *ProfileItem, ignoreValue bool) bool {
	if pi.OType != pi2.OType {
		return false
	}
	if pi.Oid != pi2.Oid {
		return false
	}
	if pi.Key != pi2.Key {
		return false
	}
	if !ignoreValue {
		if pi.Value == nil || pi2.Value == nil {
			panic(fmt.Errorf("value of profile item should be value.Nil not nil pointer"))
		} else if !pi.Value.Equal(pi2.Value) {
			return false
		}
	}
	if pi.Version != pi2.Version {
		return false
	}
	return true
}

func (pi *ProfileItem) UnmarshalJSON(data []byte) error {
	var fields struct {
		OType   ftypes.OType `json:"otype"`
		Oid     uint64       `json:"oid"`
		Key     string       `json:"key"`
		Version uint64       `json:"version"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling profile json: %v", err)
	}
	pi.OType = fields.OType
	pi.Oid = fields.Oid
	pi.Key = fields.Key
	pi.Version = fields.Version
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
