package time

import (
	"fmt"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	ops := []operators.Operator{timeBucketOfDay{}, dayOfWeek{}}
	for _, op := range ops {
		if err := operators.Register(op); err != nil {
			panic(err)
		}
	}
}

type timeBucketOfDay struct{}

var _ operators.Operator = timeBucketOfDay{}

func (t timeBucketOfDay) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return timeBucketOfDay{}, nil
}

func (t timeBucketOfDay) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	n, _ := kwargs.Get("name")
	name := string(n.(value.String))
	b, _ := kwargs.Get("bucket")
	bucket := b.(value.Int)
	if bucket <= 0 {
		return fmt.Errorf("bucket should be positive but found %v instead", bucket)
	}
	day := int64(24 * 3600)
	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		rowVal, _ := heads.Get("0")
		row := rowVal.(value.Dict)
		ts, _ := contextKwargs.Get("timestamp")
		timestamp := ts.(value.Int)
		if timestamp <= 0 {
			return fmt.Errorf("timestamp expected to be positive but found: %v instead", timestamp)
		}
		row.Set(name, value.Int((int64(timestamp)%day)/int64(bucket)))
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (t timeBucketOfDay) Signature() *operators.Signature {
	return operators.NewSignature("time", "addTimeBucketOfDay", true).
		Input(value.Types.Dict).
		Param("timestamp", value.Types.Int, false, false, value.Nil).
		Param("bucket", value.Types.Int, true, false, value.Nil).
		Param("name", value.Types.String, true, false, value.Nil)
}

type dayOfWeek struct{}

var _ operators.Operator = dayOfWeek{}

func (d dayOfWeek) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return dayOfWeek{}, nil
}

func (d dayOfWeek) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	n, _ := kwargs.Get("name")
	name := string(n.(value.String))
	//name := string(kwargs["name"].(value.String))
	week := int64(7 * 24 * 3600)
	day := int64(24 * 3600)
	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		rowVal, _ := heads.Get("0")
		row := rowVal.(value.Dict)
		ts, _ := contextKwargs.Get("timestamp")
		timestamp := ts.(value.Int)
		//timestamp := contextKwargs["timestamp"].(value.Int)
		if timestamp <= 0 {
			return fmt.Errorf("timestamp should be positive but got: %v instead", timestamp)
		}
		row.Set(name, value.Int((int64(timestamp)%week)/(day)))
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (d dayOfWeek) Signature() *operators.Signature {
	return operators.NewSignature("time", "addDayOfWeek", true).
		Input(value.Types.Dict).
		Param("timestamp", value.Types.Int, false, false, value.Nil).
		Param("name", value.Types.String, true, false, value.Nil)
}
