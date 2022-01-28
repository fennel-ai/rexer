package aggregate

import (
	"fennel/engine/ast"
	"fennel/instance"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	modelAgg "fennel/model/aggregate"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

func Store(instance instance.Instance, agg aggregate.Aggregate) error {
	if !aggregate.IsValid(agg.Type) {
		return fmt.Errorf("invalid aggregate type: %v", agg.Type)
	}

	querySer, err := ast.Marshal(agg.Query)
	if err != nil {
		return fmt.Errorf("can not marshal aggregate query: %v", err)
	}
	optionSer, err := proto.Marshal(&agg.Options)
	if err != nil {
		return fmt.Errorf("can not marshal aggregate options: %v", err)
	}
	if agg.Timestamp == 0 {
		agg.Timestamp = ftypes.Timestamp(time.Now().Unix())
	}
	return modelAgg.Store(instance, agg.Type, agg.Name, querySer, agg.Timestamp, optionSer)
}

func Retrieve(instance instance.Instance, aggtype ftypes.AggType, aggname ftypes.AggName) (aggregate.Aggregate, error) {
	empty := aggregate.Aggregate{}
	if !aggregate.IsValid(aggtype) {
		return empty, fmt.Errorf("invalid aggregate type: %v", aggtype)
	}
	if len(aggname) == 0 {
		return empty, fmt.Errorf("aggregate name can not be of length zero")
	}
	aggser, err := modelAgg.Retrieve(instance, aggtype, aggname)
	if err != nil {
		return empty, err
	}
	return aggregate.FromAggregateSer(aggser)
}

// RetrieveAll returns all aggregates of given aggtype
func RetrieveAll(instance instance.Instance, aggtype ftypes.AggType) ([]aggregate.Aggregate, error) {
	if !aggregate.IsValid(aggtype) {
		return nil, fmt.Errorf("invalid aggregate type: %v", aggtype)
	}

	retSer, err := modelAgg.RetrieveAll(instance, aggtype)
	if err != nil {
		return nil, err
	}
	ret := make([]aggregate.Aggregate, len(retSer))
	for i, ser := range retSer {
		ret[i], err = aggregate.FromAggregateSer(ser)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}
