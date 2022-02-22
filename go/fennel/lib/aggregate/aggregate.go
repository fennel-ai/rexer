package aggregate

import (
	"fmt"
	"strings"

	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"

	"google.golang.org/protobuf/proto"
)

var ValidTypes = []ftypes.AggType{
	"count",
	"timeseries_count",
	"average",
	"list",
	"min",
	"max",
	"stddev",
}

type Aggregate struct {
	Name      ftypes.AggName
	Query     ast.Ast
	Timestamp ftypes.Timestamp
	Options   AggOptions
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
	if !IsValid(ftypes.AggType(agg.Options.AggType)) {
		return fmt.Errorf("invalid aggregate type, valid types are: %v", ValidTypes)
	}
	if len(agg.Name) == 0 {
		return fmt.Errorf("aggregate name can not be of zero length")
	}
	options := agg.Options
	aggtype := agg.Options.AggType
	switch strings.ToLower(aggtype) {
	case "count", "average", "min", "max", "stddev", "list":
		if options.Duration == 0 {
			return fmt.Errorf("duration can not be zero for %s", aggtype)
		}
		if options.Window != 0 || options.Limit != 0 {
			return fmt.Errorf("retention, window and limit should all be zero for %v", aggtype)
		}
	case "timeseries_count":
		if options.Window != ftypes.Window_HOUR && options.Window != ftypes.Window_DAY {
			return fmt.Errorf("valid windows for time series are 'HOUR' or 'DAY' but got: '%v' instead", options.Window)
		}
		if options.Limit == 0 {
			return fmt.Errorf("limit can not be zero for time series counters")
		}
		if options.Duration != 0 {
			return fmt.Errorf("duration & limit are not relevant for time series and should be set to zero")
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
	return agg.Query.Equals(other.Query) && proto.Equal(&agg.Options, &other.Options)
}

type AggregateSer struct {
	Name      ftypes.AggName   `db:"name"`
	QuerySer  []byte           `db:"query_ser"`
	Timestamp ftypes.Timestamp `db:"timestamp"`
	OptionSer []byte           `db:"options_ser"`
	Active    bool             `db:"active"`
}

func FromAggregateSer(ser AggregateSer) (Aggregate, error) {
	var agg Aggregate
	agg.Timestamp = ser.Timestamp
	agg.Name = ser.Name
	if err := ast.Unmarshal(ser.QuerySer, &agg.Query); err != nil {
		return Aggregate{}, err
	}
	if err := proto.Unmarshal(ser.OptionSer, &agg.Options); err != nil {
		return Aggregate{}, err
	}
	return agg, nil
}

type GetAggValueRequest struct {
	AggName ftypes.AggName `json:"Name"`
	Key     value.Value    `json:"Key"`
}

type notFound int

func (_ notFound) Error() string {
	return "aggregate not found"
}

var ErrNotFound = notFound(1)
var _ error = ErrNotFound
