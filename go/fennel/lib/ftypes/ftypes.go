package ftypes

type RealmID uint32

func (t RealmID) Value() uint32 {
	return uint32(t)
}

type IDType uint64
type OidType string
type OType string

type ActionType string

// TODO(REX-1157): Consider moving back to uint64
type Timestamp uint32
type RequestID string

type AggType string
type AggName string
type AggId uint32
type Source string

type ModelName string
type ModelVersion string

type UserId uint32
