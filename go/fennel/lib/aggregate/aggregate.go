package aggregate

import (
	"errors"
	"fmt"
	"strings"

	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"
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
	"knn",
}

const (
	SOURCE_ACTION  = ftypes.Source("action")
	SOURCE_PROFILE = ftypes.Source("profile")
)

var ErrNotFound = errors.New("aggregate not found")
var ErrNotActive = errors.New("aggregate is not active")

type Aggregate struct {
	Name      ftypes.AggName
	Source    ftypes.Source
	Query     ast.Ast
	Timestamp ftypes.Timestamp
	Options   Options
	Id        ftypes.AggId
	Active    bool
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
	case "knn":
		if len(options.Durations) > 0 {
			return fmt.Errorf("no durations should be provided for %s", aggtype)
		}
		if options.Window != 0 || options.Limit != 0 {
			return fmt.Errorf("window, limit should all be zero for %v", aggtype)
		}
		if agg.Options.Dim <= 0 {
			return fmt.Errorf("dim must be greater than zero for %v", aggtype)
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
	if agg.Source != other.Source {
		return false
	}
	return agg.Query.Equals(other.Query) && agg.Options.Equals(other.Options)
}

func (agg Aggregate) IsOffline() bool {
	return agg.Options.CronSchedule != ""
}

func (agg Aggregate) IsForever() bool {
	return (agg.Options.Durations == nil || len(agg.Options.Durations) == 0) && agg.Options.AggType != "timeseries_sum"
}

type Options struct {
	AggType         ftypes.AggType
	Durations       []uint64
	Window          ftypes.Window
	Limit           uint64
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
	if o.CronSchedule != other.CronSchedule {
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
