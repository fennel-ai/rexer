package aggregate

import (
	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fmt"
	"google.golang.org/protobuf/proto"
	"strings"
)

var ValidTypes = []ftypes.AggType{
	"rolling_counter",
	"timeseries_counter",
	"stream",
}

type Aggregate struct {
	CustID    ftypes.CustID
	Type      ftypes.AggType
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
	if !IsValid(agg.Type) {
		return fmt.Errorf("invalid aggregate type, valid types are: %v", ValidTypes)
	}
	if len(agg.Name) == 0 {
		return fmt.Errorf("aggregate name can not be of zero length")
	}
	options := agg.Options
	switch strings.ToLower(string(agg.Type)) {
	case "rolling_counter":
		if options.Duration == 0 {
			return fmt.Errorf("duration can not be zero for rolling counters")
		}
		if options.Window != 0 || options.Limit != 0 {
			return fmt.Errorf("retention, window and limit should all be zero for rolling counters")
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
		return fmt.Errorf("unsupported aggregation type: %v", agg.Type)
	}
	return nil
}

func ToProtoAggregate(agg Aggregate) (ProtoAggregate, error) {
	pquery, err := ast.ToProtoAst(agg.Query)
	if err != nil {
		return ProtoAggregate{}, err
	}
	return ProtoAggregate{
		CustId:    uint64(agg.CustID),
		AggType:   string(agg.Type),
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
	return Aggregate{
		CustID:    ftypes.CustID(pagg.CustId),
		Type:      ftypes.AggType(strings.ToLower(pagg.AggType)),
		Name:      ftypes.AggName(strings.ToLower(pagg.AggName)),
		Query:     query,
		Timestamp: ftypes.Timestamp(pagg.Timestamp),
		Options:   *pagg.Options,
	}, nil
}

type AggregateSer struct {
	CustID    ftypes.CustID    `db:"cust_id"`
	Type      ftypes.AggType   `db:"aggregate_type"`
	Name      ftypes.AggName   `db:"name"`
	QuerySer  []byte           `db:"query_ser"`
	Timestamp ftypes.Timestamp `db:"timestamp"`
	OptionSer []byte           `db:"options_ser"`
}

func (a Aggregate) Equals(b Aggregate) bool {
	if a.CustID != b.CustID || a.Type != b.Type || a.Name != b.Name || a.Timestamp != b.Timestamp {
		return false
	}
	return a.Query == b.Query && proto.Equal(&a.Options, &b.Options)
}

func FromAggregateSer(ser AggregateSer) (Aggregate, error) {
	var agg Aggregate
	agg.CustID = ser.CustID
	agg.Timestamp = ser.Timestamp
	agg.Name = ser.Name
	agg.Type = ser.Type
	if err := ast.Unmarshal(ser.QuerySer, &agg.Query); err != nil {
		return Aggregate{}, err
	}
	if err := proto.Unmarshal(ser.OptionSer, &agg.Options); err != nil {
		return Aggregate{}, err
	}
	return agg, nil
}

type notFound int

func (_ notFound) Error() string {
	return "aggregate not found"
}

var ErrNotFound = notFound(1)
var _ error = ErrNotFound
