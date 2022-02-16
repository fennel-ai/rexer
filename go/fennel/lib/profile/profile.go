package profile

import (
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

func (pi *ProfileItem) UnmarshalJSON(data []byte) error {
	var otype, key string
	var oid, version int64
	var val value.Value
	var errors []error
	handler := func(idx int, vdata []byte, vtype jsonparser.ValueType, err error) {
		if err != nil {
			errors = append(errors, err)
			return
		}
		switch idx {
		case 0:
			otype, err = jsonparser.ParseString(vdata)
		case 1:
			oid, err = jsonparser.ParseInt(vdata)
		case 2:
			key, err = jsonparser.ParseString(vdata)
		case 3:
			val, err = value.FromJSON(vdata)
		case 4:
			version, err = jsonparser.ParseInt(vdata)
		default:
			err = fmt.Errorf("unknown index")
		}
		if err != nil {
			errors = append(errors, err)
		}
	}
	paths := [][]string{{"OType"}, {"Oid"}, {"Key"}, {"Value"}, {"Version"}}
	jsonparser.EachKey(data, handler, paths...)
	if len(errors) != 0 {
		// should this combine errors instead of returning only first error?
		return fmt.Errorf("failed to parse profile json: %v", errors[0])
	}
	pi.OType = ftypes.OType(otype)
	pi.Oid = uint64(oid)
	pi.Key = key
	pi.Value = val
	pi.Version = uint64(version)
	return nil
}

func (pfr *ProfileFetchRequest) UnmarshalJSON(data []byte) error {
	var otype, key string
	var oid, version int64
	var errors []error
	handler := func(idx int, vdata []byte, vtype jsonparser.ValueType, err error) {
		if err != nil {
			errors = append(errors, err)
			return
		}
		switch idx {
		case 0:
			otype, err = jsonparser.ParseString(vdata)
		case 1:
			oid, err = jsonparser.ParseInt(vdata)
		case 2:
			key, err = jsonparser.ParseString(vdata)
		case 3:
			version, err = jsonparser.ParseInt(vdata)
		default:
			err = fmt.Errorf("unknown index")
		}
		if err != nil {
			errors = append(errors, err)
		}
	}
	paths := [][]string{{"OType"}, {"Oid"}, {"Key"}, {"Version"}}
	jsonparser.EachKey(data, handler, paths...)
	if len(errors) != 0 {
		// should this combine errors instead of returning only first error?
		return fmt.Errorf("failed to parse profile fetch request json: %v", errors[0])
	}
	pfr.OType = ftypes.OType(otype)
	pfr.Oid = uint64(oid)
	pfr.Key = key
	pfr.Version = uint64(version)
	return nil
}
