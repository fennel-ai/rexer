package lib

import (
	"fmt"
)

const (
	PORT = 2425
)

type Key []OidType

func Windows() []Window {
	return []Window{Window_HOUR, Window_DAY, Window_WEEK, Window_MONTH, Window_QUARTER, Window_YEAR, Window_FOREVER}
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

type GetRateRequest struct {
	NumCounterType CounterType
	DenCounterType CounterType
	NumKey         Key
	DenKey         Key
	Window         Window
	Timestamp      Timestamp
	LowerBound     bool
}

func FromProtoGetRateRequest(pgrr *ProtoGetRateRequest) GetRateRequest {
	return GetRateRequest{
		pgrr.NumCounterType,
		pgrr.DenCounterType,
		toKey(pgrr.NumKey),
		toKey(pgrr.DenKey),
		pgrr.Window,
		Timestamp(pgrr.Timestamp),
		pgrr.LowerBound,
	}
}
func ToProtoGetRateRequest(grr *GetRateRequest) ProtoGetRateRequest {
	return ProtoGetRateRequest{
		NumCounterType: grr.NumCounterType,
		DenCounterType: grr.DenCounterType,
		NumKey:         fromKey(grr.NumKey),
		DenKey:         fromKey(grr.DenKey),
		Window:         grr.Window,
		Timestamp:      uint64(grr.Timestamp),
		LowerBound:     grr.LowerBound,
	}
}
func (r GetRateRequest) Validate() error {
	if r.NumCounterType == 0 {
		return fmt.Errorf("num counter type can not be zero")
	}
	if r.DenCounterType == 0 {
		return fmt.Errorf("den counter type can not be zero")
	}
	if r.Window == 0 {
		return fmt.Errorf("counter window can not be zero")
	}
	if len(r.NumKey) == 0 {
		return fmt.Errorf("num counter key can not be empty")
	}
	if len(r.DenKey) == 0 {
		return fmt.Errorf("den counter key can not be empty")
	}
	return nil
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
