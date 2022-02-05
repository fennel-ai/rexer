package aggregate

import (
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	modelAgg "fennel/model/aggregate"
	"fennel/tier"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

func Store(instance tier.Tier, agg aggregate.Aggregate) error {
	if err := agg.Validate(); err != nil {
		return err
	}

	// Check if agg already exists in db
	agg2, err := Retrieve(instance, agg.Type, agg.Name)
	// Only error that should happen is when agg is not present
	if err != nil && err != aggregate.ErrNotFound {
		return err
	} else if err == nil {
		// if already present, check if query and options are the same
		// if they are different, return error
		// if they are the same, do nothing
		// TODO: maybe not use proto.Equal here
		if agg.Query == agg2.Query && proto.Equal(&agg.Options, &agg2.Options) {
			return nil
		} else {
			return fmt.Errorf("already present but with different query/options")
		}
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

func Retrieve(instance tier.Tier, aggtype ftypes.AggType, aggname ftypes.AggName) (aggregate.Aggregate, error) {
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
func RetrieveAll(instance tier.Tier, aggtype ftypes.AggType) ([]aggregate.Aggregate, error) {
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
