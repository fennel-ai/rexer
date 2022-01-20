package profile

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
)

const (
	User  ftypes.OType = 1
	Video              = 2
)

type ProfileItem struct {
	OType   uint32
	Oid     uint64
	Key     string
	Version uint64
	Value   value.Value
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
