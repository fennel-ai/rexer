package ftypes

type RealmID uint32

func (t RealmID) Value() uint32 {
	return uint32(t)
}

type IDType uint64
type OidType string
type OType string

type ActionType string
type Timestamp uint64
type RequestID uint64

type AggType string
type AggName string
type AggId uint32

type ModelName string
type ModelVersion string
