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
	OType   ftypes.OType `db:"otype"`
	Oid     uint64       `db:"oid"`
	Key     string       `db:"zkey"`
	Version uint64       `db:"version"`
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

func FromJSON(data []byte) (ProfileItem, error) {
	var zero ProfileItem
	otype, err := jsonparser.GetString(data, "OType")
	if err != nil {
		return zero, fmt.Errorf("failed to parse OType json: %v", err)
	}
	oid, err := jsonparser.GetInt(data, "Oid")
	if err != nil {
		return zero, fmt.Errorf("failed to parse Oid json: %v", err)
	}
	key, err := jsonparser.GetString(data, "Key")
	if err != nil {
		return zero, fmt.Errorf("failed to parse Key json: %v", err)
	}
	vdata, _, _, err := jsonparser.Get(data, "Value")
	if err != nil {
		return zero, fmt.Errorf("failed to get Value json: %v", err)
	}
	val, err := value.FromJSON(vdata)
	if err != nil {
		return zero, fmt.Errorf("failed to parse Value json: %v", err)
	}
	version, err := jsonparser.GetInt(data, "Version")
	if err != nil {
		return zero, fmt.Errorf("failed to parse Version json: %v", err)
	}
	return ProfileItem{
		OType:   ftypes.OType(otype),
		Oid:     uint64(oid),
		Key:     key,
		Value:   val,
		Version: uint64(version),
	}, nil
}

func ToJSON(pi *ProfileItem) ([]byte, error) {
	return json.Marshal(pi)
}
