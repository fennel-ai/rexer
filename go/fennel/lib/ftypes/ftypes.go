package ftypes

type RealmID uint32

func (t RealmID) Value() uint32 {
	return uint32(t)
}

type OidType uint64
type OType string

type ActionType string
type Timestamp uint64
type RequestID uint64

type AggType string
type AggName string

type ModelName string
type ModelVersion string
