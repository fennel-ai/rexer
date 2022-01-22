package profile

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"

	"google.golang.org/protobuf/proto"
)

const (
	User  ftypes.OType = 1
	Video              = 2
)

type ProfileItem struct {
	OType   uint32      `db:"otype"`
	Oid     uint64      `db:"oid"`
	Key     string      `db:"zkey"`
	Version uint64      `db:"version"`
	Value   value.Value `db:"value"`
}

func NewProfileItem(otype uint32, oid uint64, k string, version uint64) ProfileItem {
	return ProfileItem{
		otype, oid, k, version, value.Nil,
	}
}

func FromProtoProfileItem(ppr *ProtoProfileItem) (ProfileItem, error) {
	v, err := value.FromProtoValue(ppr.Value)
	if err != nil {
		return ProfileItem{}, err
	}
	return ProfileItem{
		ppr.OType,
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
		OType:   pi.OType,
		Oid:     pi.Oid,
		Key:     pi.Key,
		Version: pi.Version,
		Value:   &pv,
	}, nil
}

type ProfileItemSer struct {
	OType   uint32 `db:"otype"`
	Oid     uint64 `db:"oid"`
	Key     string `db:"zkey"`
	Version uint64 `db:"version"`
	Value   []byte `db:"value"`
}

// Converts a ProfileItemSer to ProfileItem
func ToProfileItem(ser *ProfileItemSer) (*ProfileItem, error) {
	pr := NewProfileItem(ser.OType, ser.Oid, ser.Key, ser.Version)

	var pval value.PValue
	if err := proto.Unmarshal(ser.Value, &pval); err != nil {
		return &pr, nil
	}

	val, err := value.FromProtoValue(&pval)
	if err != nil {
		return &pr, nil
	}

	pr.Value = val
	return &pr, nil
}

type ProfileFetchRequest struct {
	OType   uint32 `db:"otype"`
	Oid     uint64 `db:"oid"`
	Key     string `db:"zkey"`
	Version uint64 `db:"version"`
}

func FromProtoProfileFetchRequest(ppfr *ProtoProfileFetchRequest) ProfileFetchRequest {
	return ProfileFetchRequest{
		ppfr.OType,
		ppfr.Oid,
		ppfr.Key,
		ppfr.Version,
	}
}

// Should this use pointer argument instead?
func ToProtoProfileFetchRequest(pfr ProfileFetchRequest) ProtoProfileFetchRequest {
	return ProtoProfileFetchRequest{
		OType:   pfr.OType,
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
	if pi.Oid == 0 {
		return fmt.Errorf("oid can not be zero")
	}
	if pi.OType == 0 {
		return fmt.Errorf("otype can not be zero")
	}
	if len(pi.Key) == 0 {
		return fmt.Errorf("key can not be empty")
	}
	return nil
}
