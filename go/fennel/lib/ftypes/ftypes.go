package ftypes

type OidType uint64
type OType uint32

type ActionType uint32
type Timestamp uint64
type RequestID uint64

type Key []OidType

func Windows() []Window {
	return []Window{Window_HOUR, Window_DAY, Window_WEEK, Window_MONTH, Window_QUARTER, Window_YEAR, Window_FOREVER}
}

func ToKey(k []uint64) Key {
	ret := make([]OidType, len(k))
	for i, n := range k {
		ret[i] = OidType(n)
	}
	return ret
}
func FromKey(k Key) []uint64 {
	ret := make([]uint64, len(k))
	for i, n := range k {
		ret[i] = uint64(n)
	}
	return ret
}
