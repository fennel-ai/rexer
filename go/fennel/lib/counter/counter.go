package counter

import (
	"fennel/lib/ftypes"
	"fmt"
)

type GetCountRequest struct {
	CounterType CounterType
	Window      ftypes.Window
	Key         ftypes.Key
	Timestamp   ftypes.Timestamp
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
		ftypes.ToKey(pgcr.Key),
		ftypes.Timestamp(pgcr.Timestamp),
	}
}
func ToProtoGetCountRequest(gcr *GetCountRequest) ProtoGetCountRequest {
	return ProtoGetCountRequest{
		CounterType: gcr.CounterType,
		Window:      gcr.Window,
		Key:         ftypes.FromKey(gcr.Key),
		Timestamp:   uint64(gcr.Timestamp),
	}
}

type GetRateRequest struct {
	NumCounterType CounterType
	DenCounterType CounterType
	NumKey         ftypes.Key
	DenKey         ftypes.Key
	Window         ftypes.Window
	Timestamp      ftypes.Timestamp
	LowerBound     bool
}

func FromProtoGetRateRequest(pgrr *ProtoGetRateRequest) GetRateRequest {
	return GetRateRequest{
		pgrr.NumCounterType,
		pgrr.DenCounterType,
		ftypes.ToKey(pgrr.NumKey),
		ftypes.ToKey(pgrr.DenKey),
		pgrr.Window,
		ftypes.Timestamp(pgrr.Timestamp),
		pgrr.LowerBound,
	}
}
func ToProtoGetRateRequest(grr *GetRateRequest) ProtoGetRateRequest {
	return ProtoGetRateRequest{
		NumCounterType: grr.NumCounterType,
		DenCounterType: grr.DenCounterType,
		NumKey:         ftypes.FromKey(grr.NumKey),
		DenKey:         ftypes.FromKey(grr.DenKey),
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
