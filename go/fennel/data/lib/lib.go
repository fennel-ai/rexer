package lib

import (
	"fmt"
	"math"
)

type OidType uint64
type Timestamp uint64
type RequestID uint64

const (
	PORT = 2425
)

type OType uint32

const (
	User  OType = 1
	Video       = 2
)

type ActionType uint32

const (
	Like  ActionType = 1
	Share            = 2
	View             = 3
)

type Key []OidType

func Windows() []Window {
	return []Window{Window_HOUR, Window_DAY, Window_WEEK, Window_MONTH, Window_QUARTER, Window_YEAR, Window_FOREVER}
}

const Z_95 = 1.96

func Wilson(num uint64, den uint64, lower bool) float64 {
	// (p + Z_95²/2n ± Z_95√p(1 – p)/n + Z_95²/4n²) / (1 + Z_95²/n)
	if den == 0 {
		return 0
	}

	p := float64(num) / float64(den)
	n := float64(den)
	base := p + (Z_95*Z_95)/(2*n)
	plusminus := Z_95 * math.Sqrt(p*(1-p)/n+(Z_95*Z_95)/(4*n*n))
	normalize := 1 + (Z_95*Z_95)/n
	if lower {
		return (base - plusminus) / normalize
	} else {
		return (base + plusminus) / normalize
	}
}

type GetCountRequest struct {
	CounterType CounterType
	Window      Window
	Key         Key
	Timestamp   Timestamp
}

func (r GetCountRequest) Validate() error {
	if r.CounterType == 0 {
		return fmt.Errorf("counter type can not be zero")
	}
	if r.Window == 0 {
		return fmt.Errorf("counter window can not be zero")
	}
	if len(r.Key) == 0 {
		return fmt.Errorf("counter key can not be empty")
	}
	return nil
}

func FromProtoGetCountRequest(pgcr *ProtoGetCountRequest) GetCountRequest {
	return GetCountRequest{
		pgcr.CounterType,
		pgcr.Window,
		toKey(pgcr.Key),
		Timestamp(pgcr.Timestamp),
	}
}
func ToProtoGetCountRequest(gcr *GetCountRequest) ProtoGetCountRequest {
	return ProtoGetCountRequest{
		CounterType: gcr.CounterType,
		Window:      gcr.Window,
		Key:         fromKey(gcr.Key),
		Timestamp:   uint64(gcr.Timestamp),
	}
}

func toKey(k []uint64) Key {
	ret := make([]OidType, len(k))
	for i, n := range k {
		ret[i] = OidType(n)
	}
	return Key(ret)
}
func fromKey(k Key) []uint64 {
	ret := make([]uint64, len(k))
	for i, n := range k {
		ret[i] = uint64(n)
	}
	return ret
}
