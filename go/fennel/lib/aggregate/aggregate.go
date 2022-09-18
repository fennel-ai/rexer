package aggregate

import (
	"errors"
	"fmt"
	"strings"

	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"

	"google.golang.org/protobuf/proto"
)

const (
	SUM            ftypes.AggType = "sum"
	TIMESERIES_SUM ftypes.AggType = "timeseries_sum"
	AVERAGE        ftypes.AggType = "average"
	MIN            ftypes.AggType = "min"
	MAX            ftypes.AggType = "max"
	STDDEV         ftypes.AggType = "stddev"
	LIST           ftypes.AggType = "list"
	RATE           ftypes.AggType = "rate"
	TOPK           ftypes.AggType = "topk"
	CF             ftypes.AggType = "cf"
	KNN            ftypes.AggType = "knn"
	VAE            ftypes.AggType = "vae"
	RQL            string         = "rql"
	PANDAS         string         = "pandas"
)

var ValidTypes = []ftypes.AggType{
	SUM,
	TIMESERIES_SUM,
	AVERAGE,
	MIN,
	MAX,
	STDDEV,
	LIST,
	RATE,
	TOPK,
	CF,
	VAE,
	KNN,
}

var ValidOfflineAggregates = []ftypes.AggType{
	TOPK,
	CF,
}

var ValidAutoMlAggregates = []ftypes.AggType{
	VAE,
}

var ValidAggregateModes = []string{
	RQL, PANDAS,
}

const (
	SOURCE_ACTION  = ftypes.Source("action")
	SOURCE_PROFILE = ftypes.Source("profile")
)

var ErrNotFound = errors.New("aggregate not found")
var ErrNotActive = errors.New("aggregate is not active")

type Aggregate struct {
	Name        ftypes.AggName
	Source      ftypes.Source
	Mode        string
	PythonQuery []byte
	Query       ast.Ast
	Timestamp   ftypes.Timestamp
	Options     Options
	Id          ftypes.AggId
	Active      bool
}

func IsValid(s ftypes.AggType, validTypes []ftypes.AggType) bool {
	sl := ftypes.AggType(strings.ToLower(string(s)))
	for _, m := range validTypes {
		if sl == m {
			return true
		}
	}
	return false
}

func IsValidMode(s string) bool {
	for _, m := range ValidAggregateModes {
		if s == m {
			return true
		}
	}
	return false
}

func (agg Aggregate) Validate() error {
	if !IsValid(agg.Options.AggType, ValidTypes) {
		return fmt.Errorf("invalid aggregate type: '%v'; valid types are: %v", agg.Options.AggType, ValidTypes)
	}
	if len(agg.Name) == 0 {
		return fmt.Errorf("aggregate name can not be of zero length")
	}
	if !IsValidMode(agg.Mode) {
		return fmt.Errorf("invalid aggregate mode: '%v'; valid modes are: %v", agg.Mode, ValidAggregateModes)
	}
	options := agg.Options
	aggtype := agg.Options.AggType
	switch ftypes.AggType(strings.ToLower(string(aggtype))) {
	case SUM, AVERAGE, MIN, MAX, STDDEV, LIST:
		if len(options.Durations) < 1 {
			return fmt.Errorf("at least one duration must be provided for %s", aggtype)
		}
		for _, d := range options.Durations {
			if d == 0 {
				return fmt.Errorf("duration can not be zero for %s", aggtype)
			}
		}
		if options.Window != 0 || options.Limit != 0 || options.Normalize {
			return fmt.Errorf("window, limit, normalize should all be zero for %v", aggtype)
		}
	case TOPK, CF:
		if len(options.Durations) < 1 {
			return fmt.Errorf("at least one duration must be provided for %s", aggtype)
		}
		for _, d := range options.Durations {
			if d == 0 {
				return fmt.Errorf("duration can not be zero for %s", aggtype)
			}
		}
		if options.Window != 0 || options.Normalize {
			return fmt.Errorf("window, normalize should all be zero for %v", aggtype)
		}
		if options.Limit == 0 {
			return fmt.Errorf("limit should be non-zero for %v", aggtype)
		}
	case RATE:
		for _, d := range options.Durations {
			if d == 0 {
				return fmt.Errorf("duration can not be zero for %s", aggtype)
			}
		}
		if options.Window != 0 || options.Limit != 0 {
			return fmt.Errorf("window, limit should all be zero for %v", aggtype)
		}
	case KNN:
		if len(options.Durations) > 0 {
			return fmt.Errorf("no durations should be provided for %s", aggtype)
		}
		if options.Window != 0 || options.Limit != 0 {
			return fmt.Errorf("window, limit should all be zero for %v", aggtype)
		}
		if agg.Options.Dim <= 0 {
			return fmt.Errorf("dim must be greater than zero for %v", aggtype)
		}
	case VAE:
		if len(options.Durations) == 0 {
			return fmt.Errorf("at least one duration must be provided for %s", aggtype)
		}
		for _, d := range options.Durations {
			if d == 0 {
				return fmt.Errorf("duration can not be zero for %s", aggtype)
			}
		}
		if options.Limit == 0 {
			return fmt.Errorf("limit should be non-zero for %v", aggtype)
		}
	case TIMESERIES_SUM:
		if options.Window != ftypes.Window_HOUR && options.Window != ftypes.Window_DAY {
			return fmt.Errorf("valid windows for time series are 'HOUR' or 'DAY' but got: '%v' instead", options.Window)
		}
		if options.Limit == 0 {
			return fmt.Errorf("limit can not be zero for time series sum")
		}
		if len(options.Durations) != 0 {
			return fmt.Errorf("durations are not relevant for time series and should be set to empty list")
		}
		if options.Normalize {
			return fmt.Errorf("normalize is not relevant for time series and should be set to zero")
		}
	default:
		return fmt.Errorf("unsupported aggregation type: %v", agg.Options.AggType)
	}
	return nil
}

func (agg Aggregate) IsProfileBased() bool {
	return agg.Source == SOURCE_PROFILE
}

func (agg Aggregate) IsActionBased() bool {
	return agg.Source == SOURCE_ACTION
}

func (agg Aggregate) Equals(other Aggregate) bool {
	if agg.Options.AggType != other.Options.AggType || agg.Name != other.Name || agg.Timestamp != other.Timestamp {
		return false
	}
	if agg.Source != other.Source {
		return false
	}
	return agg.Query.Equals(other.Query) && agg.Options.Equals(other.Options)
}

func (agg Aggregate) IsOffline() bool {
	return agg.Options.CronSchedule != "" && IsValid(agg.Options.AggType, ValidOfflineAggregates)
}

func (agg Aggregate) IsAutoML() bool {
	return agg.Options.CronSchedule != "" && IsValid(agg.Options.AggType, ValidAutoMlAggregates)
}

func (agg Aggregate) IsForever() bool {
	return len(agg.Options.Durations) == 0 && agg.Options.AggType != TIMESERIES_SUM
}

func (agg Aggregate) IsOnline() bool {
	return !agg.IsOffline() && !agg.IsAutoML() && !agg.IsForever()
}

type Options struct {
	AggType         ftypes.AggType
	Durations       []uint32
	Window          ftypes.Window
	Limit           uint32
	Normalize       bool
	CronSchedule    string
	Dim             uint32
	HyperParameters string
}

func (o Options) Equals(other Options) bool {
	if o.AggType != other.AggType {
		return false
	}
	if len(o.Durations) != len(other.Durations) {
		return false
	}
	for i := range o.Durations {
		if o.Durations[i] != other.Durations[i] {
			return false
		}
	}
	if o.Window != other.Window {
		return false
	}
	if o.Limit != other.Limit {
		return false
	}
	if o.Normalize != other.Normalize {
		return false
	}
	if o.Dim != other.Dim {
		return false
	}
	if o.HyperParameters != other.HyperParameters {
		return false
	}
	return true
}

type GetAggValueRequest struct {
	AggName ftypes.AggName `json:"Name"`
	Key     value.Value    `json:"Key"`
	Kwargs  value.Dict     `json:"Kwargs"`
}

// AggregateSer should not be used outside of package `fennel/model/aggregate` and `tier.go` file
type AggregateSer struct {
	Name      ftypes.AggName   `db:"name"`
	Source    ftypes.Source    `db:"source"`
	Mode      string           `db:"mode"`
	QuerySer  []byte           `db:"query_ser"`
	Timestamp ftypes.Timestamp `db:"timestamp"`
	OptionSer []byte           `db:"options_ser"`
	Active    bool             `db:"active"`
	Id        ftypes.AggId     `db:"id"`
}

func (ser AggregateSer) ToAggregate() (Aggregate, error) {
	var agg Aggregate
	agg.Timestamp = ser.Timestamp
	agg.Name = ser.Name
	var popt AggOptions
	if err := proto.Unmarshal(ser.OptionSer, &popt); err != nil {
		return Aggregate{}, err
	}
	agg.Options = FromProtoOptions(&popt)
	agg.Active = ser.Active
	agg.Id = ser.Id
	agg.Source = ser.Source
	agg.Mode = ser.Mode
	if agg.Mode == RQL {
		if err := ast.Unmarshal(ser.QuerySer, &agg.Query); err != nil {
			return Aggregate{}, err
		}
	} else if agg.Mode == PANDAS {
		agg.PythonQuery = ser.QuerySer
	} else {
		return Aggregate{}, fmt.Errorf("unknown mode %s", agg.Mode)
	}
	return agg, nil
}
