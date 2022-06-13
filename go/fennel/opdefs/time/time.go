package time

import (
	"context"
	"fennel/engine/operators"
	"fennel/lib/value"
	"fmt"
	"strings"
	"time"
)

func init() {
	if err := operators.Register(Extractor{}); err != nil {
		panic(err)
	}
}

func year(t time.Time) int {
	return t.Year()
}

func month(t time.Time) int {
	return int(t.Month())
}

func day(t time.Time) int {
	return t.Day()
}

func hour(t time.Time) int {
	return t.Hour()
}

func minute(t time.Time) int {
	return t.Minute()
}

func second(t time.Time) int {
	return t.Second()
}

func weekday(t time.Time) int {
	return int(t.Weekday())
}

func yearday(t time.Time) int {
	return t.YearDay()
}

var extractorMap = map[string]func(time.Time) int{
	"year":    year,
	"month":   month,
	"day":     day,
	"hour":    hour,
	"minute":  minute,
	"second":  second,
	"weekday": weekday,
	"yearday": yearday,
}

type Extractor struct{}

func (e Extractor) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return Extractor{}, nil
}

func (e Extractor) Signature() *operators.Signature {
	return operators.NewSignature("time", "extract").
		ParamWithHelp("element", value.Types.String, true, false, nil, "StaticKwargs: The type of time element to extract from unix timestamp which is in seconds, one of [year,month,day,hour,minute,second,weekday,yearday]").
		ParamWithHelp("by", value.Types.Any, false, true, nil, "The field the timestamp is stored at if input is a list of dictionary").
		ParamWithHelp("field", value.Types.String, true, true, value.String(""), "StaticKwarg: String param that is used as the key to store the result")
	// TODO(siyuan): time zone
}

func (e Extractor) Apply(_ context.Context, staticKwargs operators.Kwargs, in operators.InputIter, out *value.List) error {
	element := string(staticKwargs.GetUnsafe("element").(value.String))
	field := string(staticKwargs.GetUnsafe("field").(value.String))

	extractor, ok := extractorMap[strings.ToLower(element)]
	if !ok {
		return fmt.Errorf("element to extract can only be in [year,month,day,hour,minute,second,weekday,yearday]")
	}

	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		var rawV value.Value
		by, _ := kwargs.Get("by")
		if by == nil {
			rawV = heads[0]
		} else {
			rawV = by
		}
		var v int64
		switch rawV := rawV.(type) {
		case value.Int:
			v = int64(rawV)
		case value.Double:
			v = int64(rawV)
		default:
			return fmt.Errorf("time field to extract should be a number. Got [%s]", rawV.String())
		}

		extracted := value.Int(extractor(time.Unix(v, 0)))
		if len(field) > 0 && by != nil {
			d := heads[0].(value.Dict)
			d.Set(field, extracted)
			out.Append(d)
		} else {
			out.Append(extracted)
		}
	}
	return nil
}
