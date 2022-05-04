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

var ValidTypes = []ftypes.AggType{
	"sum",
	"timeseries_sum",
	"average",
	"list",
	"min",
	"max",
	"stddev",
	"rate",
	"topk",
	"cf",
}

var ErrNotFound = errors.New("aggregate not found")

type Aggregate struct {
	Name      ftypes.AggName
	Query     ast.Ast
	Timestamp ftypes.Timestamp
	Options   Options
	// TODO: This is an internal only field and returning this back to the user
	// might confuse them. Consider creating two different structs - one returned back to the
	// user and another for internal use only
	Id ftypes.AggId
}

func IsValid(s ftypes.AggType) bool {
	sl := ftypes.AggType(strings.ToLower(string(s)))
	for _, m := range ValidTypes {
		if sl == m {
			return true
		}
	}
	return false
}

func (agg Aggregate) Validate() error {
	if !IsValid(agg.Options.AggType) {
		return fmt.Errorf("invalid aggregate type: '%v'; valid types are: %v", agg.Options.AggType, ValidTypes)
	}
	if len(agg.Name) == 0 {
		return fmt.Errorf("aggregate name can not be of zero length")
	}
	options := agg.Options
	aggtype := agg.Options.AggType
	switch strings.ToLower(string(aggtype)) {
	case "sum", "average", "min", "max", "stddev", "list":
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
	case "topk", "cf":
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
	case "rate":
		for _, d := range options.Durations {
			if d == 0 {
				return fmt.Errorf("duration can not be zero for %s", aggtype)
			}
		}
		if options.Window != 0 || options.Limit != 0 {
			return fmt.Errorf("window, limit should all be zero for %v", aggtype)
		}
	case "timeseries_sum":
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

func (agg Aggregate) Equals(other Aggregate) bool {
	if agg.Options.AggType != other.Options.AggType || agg.Name != other.Name || agg.Timestamp != other.Timestamp {
		return false
	}
	return agg.Query.Equals(other.Query) && agg.Options.Equals(other.Options)
}

func (agg Aggregate) IsOffline() bool {
	return agg.Options.CronSchedule != ""
}

type Options struct {
	AggType         ftypes.AggType
	Durations       []uint64
	Window          ftypes.Window
	Limit           uint64
	Normalize       bool
	CronSchedule    string
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
	return true
}

type AggregateSer struct {
	Name      ftypes.AggName   `db:"name"`
	QuerySer  []byte           `db:"query_ser"`
	Timestamp ftypes.Timestamp `db:"timestamp"`
	OptionSer []byte           `db:"options_ser"`
	Active    bool             `db:"active"`
	Id        ftypes.AggId     `db:"id"`
}

func FromAggregateSer(ser AggregateSer) (Aggregate, error) {
	var agg Aggregate
	agg.Timestamp = ser.Timestamp
	agg.Name = ser.Name
	if err := ast.Unmarshal(ser.QuerySer, &agg.Query); err != nil {
		return Aggregate{}, err
	}
	var popt AggOptions
	if err := proto.Unmarshal(ser.OptionSer, &popt); err != nil {
		return Aggregate{}, err
	}
	agg.Options = FromProtoOptions(&popt)

	agg.Id = ser.Id
	return agg, nil
}

type GetAggValueRequest struct {
	AggName ftypes.AggName `json:"Name"`
	Key     value.Value    `json:"Key"`
	Kwargs  value.Dict     `json:"Kwargs"`
}
