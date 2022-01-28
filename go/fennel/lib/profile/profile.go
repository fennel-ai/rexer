package profile

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"

	"google.golang.org/protobuf/proto"
)

const (
	User  ftypes.OType = "User"
	Video ftypes.OType = "Video"
)

type ProfileItem struct {
	CustID  ftypes.CustID `db:"cust_id"`
	OType   ftypes.OType  `db:"otype"`
	Oid     uint64        `db:"oid"`
	Key     string        `db:"zkey"`
	Version uint64        `db:"version"`
	Value   value.Value   `db:"value"`
}

func NewProfileItem(custid uint64, otype string, oid uint64, k string, version uint64) ProfileItem {
	return ProfileItem{
		ftypes.CustID(custid), ftypes.OType(otype), oid, k, version, value.Nil,
	}
}

func NewProfileItemSer(custid uint64, otype string, oid uint64, key string, version uint64, val []byte) ProfileItemSer {
	return ProfileItemSer{
		ftypes.CustID(custid), ftypes.OType(otype), oid, key, version, val,
	}
}

func FromProtoProfileItem(ppr *ProtoProfileItem) (ProfileItem, error) {
	v, err := value.FromProtoValue(ppr.Value)
	if err != nil {
		return ProfileItem{}, err
	}
	return ProfileItem{
		ftypes.CustID(ppr.CustID),
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
		CustID:  uint64(pi.CustID),
		OType:   string(pi.OType),
		Oid:     pi.Oid,
		Key:     pi.Key,
		Version: pi.Version,
		Value:   &pv,
	}, nil
}

type ProfileItemSer struct {
	CustID  ftypes.CustID `db:"cust_id"`
	OType   ftypes.OType  `db:"otype"`
	Oid     uint64        `db:"oid"`
	Key     string        `db:"zkey"`
	Version uint64        `db:"version"`
	Value   []byte        `db:"value"`
}

// Converts a ProfileItemSer to ProfileItem
func (ser *ProfileItemSer) ToProfileItem() (*ProfileItem, error) {
	pr := ProfileItem{ser.CustID, ser.OType, ser.Oid, ser.Key, ser.Version, value.Nil}

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
	CustID  ftypes.CustID `db:"cust_id"`
	OType   ftypes.OType  `db:"otype"`
	Oid     uint64        `db:"oid"`
	Key     string        `db:"zkey"`
	Version uint64        `db:"version"`
}

func FromProtoProfileFetchRequest(ppfr *ProtoProfileFetchRequest) ProfileFetchRequest {
	return ProfileFetchRequest{
		ftypes.CustID(ppfr.CustID),
		ftypes.OType(ppfr.OType),
		ppfr.Oid,
		ppfr.Key,
		ppfr.Version,
	}
}

func ToProtoProfileFetchRequest(pfr *ProfileFetchRequest) ProtoProfileFetchRequest {
	return ProtoProfileFetchRequest{
		CustID:  uint64(pfr.CustID),
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
	if pi.CustID == 0 {
		return fmt.Errorf("custid can not be zero")
	}
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
