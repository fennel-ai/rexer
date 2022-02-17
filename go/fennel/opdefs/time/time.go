package time

import (
	"fennel/engine/operators"
	"fennel/lib/value"
	"fmt"
)

type timeBucketOfDay struct{}

var _ operators.Operator = timeBucketOfDay{}

func (t timeBucketOfDay) Init(args value.Dict, bootargs map[string]interface{}) error {
	return nil
}

func (t timeBucketOfDay) Apply(kwargs value.Dict, in operators.InputIter, out *value.Table) error {
	name := string(kwargs["name"].(value.String))
	bucket := kwargs["bucket"].(value.Int)
	if bucket <= 0 {
		return fmt.Errorf("bucket should be positive but found %v instead", bucket)
	}
	day := int64(24 * 3600)
	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		timestamp := contextKwargs["timestamp"].(value.Int)
		if timestamp <= 0 {
			return fmt.Errorf("timestamp expected to be positive but found: %v instead", timestamp)
		}
		row[name] = value.Int((int64(timestamp) % day) / int64(bucket))
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (t timeBucketOfDay) Signature() *operators.Signature {
	return operators.NewSignature(t, "time", "addTimeBucketOfDay").
		Param("timestamp", value.Types.Int, false, false, value.Nil).
		Param("bucket", value.Types.Int, true, false, value.Nil).
		Param("name", value.Types.String, true, false, value.Nil)
}

type dayOfWeek struct{}

var _ operators.Operator = dayOfWeek{}

func (d dayOfWeek) Init(args value.Dict, bootargs map[string]interface{}) error {
	return nil
}

func (d dayOfWeek) Apply(kwargs value.Dict, in operators.InputIter, out *value.Table) error {
	name := string(kwargs["name"].(value.String))
	week := int64(7 * 24 * 3600)
	day := int64(24 * 3600)
	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		timestamp := contextKwargs["timestamp"].(value.Int)
		if timestamp <= 0 {
			return fmt.Errorf("timestamp should be positive but got: %v instead", timestamp)
		}
		row[name] = value.Int((int64(timestamp) % week) / (day))
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (d dayOfWeek) Signature() *operators.Signature {
	return operators.NewSignature(d, "time", "addDayOfWeek").
		Param("timestamp", value.Types.Int, false, false, value.Nil).
		Param("name", value.Types.String, true, false, value.Nil)
}
