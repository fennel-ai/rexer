package aggregate

import (
	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
)

var ValidTypes = []ftypes.AggType{
	"rolling_counter",
	"timeseries_counter",
	"rolling_average",
	"stream",
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
	case "rolling_counter", "rolling_average":
		if options.Duration == 0 {
			return fmt.Errorf("duration can not be zero for %s", aggtype)
		}
		if options.Window != 0 || options.Limit != 0 {
			return fmt.Errorf("retention, window and limit should all be zero for %v", aggtype)
		}
	case "timeseries_counter":
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

func ToProtoAggregate(agg Aggregate) (ProtoAggregate, error) {
	pquery, err := ast.ToProtoAst(agg.Query)
	if err != nil {
		return ProtoAggregate{}, err
	}
	return ProtoAggregate{
		AggName:   string(agg.Name),
		Query:     &pquery,
		Timestamp: uint64(agg.Timestamp),
		Options:   &agg.Options,
	}, nil
}

func FromProtoAggregate(pagg ProtoAggregate) (Aggregate, error) {
	query, err := ast.FromProtoAst(*pagg.Query)
	if err != nil {
		return Aggregate{}, err
	}
	agg := Aggregate{
		Name:      ftypes.AggName(strings.ToLower(pagg.AggName)),
		Query:     query,
		Timestamp: ftypes.Timestamp(pagg.Timestamp),
		Options:   *pagg.Options,
	}
	agg.Options.AggType = strings.ToLower(agg.Options.AggType)
	return agg, nil
}

func (a Aggregate) Equals(b Aggregate) bool {
	if a.Options.AggType != b.Options.AggType || a.Name != b.Name || a.Timestamp != b.Timestamp {
		return false
	}
	return a.Query.Equals(b.Query) && proto.Equal(&a.Options, &b.Options)
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
	AggName ftypes.AggName
	Key     value.Value
}

func FromProtoGetAggValueRequest(pr *ProtoGetAggValueRequest) (GetAggValueRequest, error) {
	key, err := value.FromProtoValue(pr.GetKey())
	if err != nil {
		return GetAggValueRequest{}, err
	}
	return GetAggValueRequest{
		AggName: ftypes.AggName(pr.GetAggName()),
		Key:     key,
	}, nil
}

func ToProtoGetAggValueRequest(gavr GetAggValueRequest) (ProtoGetAggValueRequest, error) {
	pkey, err := value.ToProtoValue(gavr.Key)
	if err != nil {
		return ProtoGetAggValueRequest{}, nil
	}
	return ProtoGetAggValueRequest{
		AggName: string(gavr.AggName),
		Key:     &pkey,
	}, nil
}

type notFound int

func (_ notFound) Error() string {
	return "aggregate not found"
}

var ErrNotFound = notFound(1)
var _ error = ErrNotFound
