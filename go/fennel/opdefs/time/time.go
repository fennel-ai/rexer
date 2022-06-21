package time

import (
	"context"
	"fennel/engine/operators"
	"fennel/lib/value"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
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

func dayofweek(t time.Time) int {
	return int(t.Weekday())
}

func dayofyear(t time.Time) int {
	return t.YearDay()
}

var extractorMap = map[string]func(time.Time) int{
	"year":      year,
	"month":     month,
	"day":       day,
	"hour":      hour,
	"minute":    minute,
	"second":    second,
	"dayofweek": dayofweek,
	"dayofyear": dayofyear,
}

type Extractor struct{}

func (e Extractor) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return Extractor{}, nil
}

func (e Extractor) Signature() *operators.Signature {
	return operators.NewSignature("time", "extract").
		ParamWithHelp("part", value.Types.String, false, false, value.String("timestamp"), "Part of time to extract from input, one of [year,month,day,hour,minute,second,dayofweek,dayofyear,timestamp,rfc3339,slot]").
		ParamWithHelp("on", value.Types.Any, false, true, nil, "The timestamp or ISO date string if input is a list of dictionary").
		ParamWithHelp("timezone", value.Types.Any, false, true, value.Int(0), "The assumed timezone when extracting, e.g. Tokyo time can be 9, 'Asia/Tokyo', or '+09:00' ").
		ParamWithHelp("field", value.Types.String, true, true, value.String(""), "StaticKwarg: String param that is used as the key to store the result").
		ParamWithHelp("slot_size", value.Types.Int, true, true, value.Int(1), "StaticKwarg: Only relevant for part being 'slot', the size of slot in seconds").
		ParamWithHelp("slot_cycle", value.Types.Int, true, true, value.Int(3600), "StaticKwarg: Only relevant for part being 'slot', the number of slots in each cycle")
}

func getTimezone(v value.Value) (*time.Location, error) {
	switch v := v.(type) {
	case value.Int:
		return time.FixedZone("fixed", int(v)*3600), nil
	case value.Double:
		return time.FixedZone("fixed", int(v)*3600), nil
	case value.String:
		strZone := string(v)
		locObj, err := time.LoadLocation(strZone)
		if err != nil {
			if len(strZone) == 6 && (strZone[0] == '+' || strZone[0] == '-') && (strZone[3] == ':') && unicode.IsDigit(int32(strZone[1])) && unicode.IsDigit(int32(strZone[2])) && unicode.IsDigit(int32(strZone[4])) && unicode.IsDigit(int32(strZone[5])) {
				hour, _ := strconv.Atoi(strZone[1:3])
				minute, _ := strconv.Atoi(strZone[4:6])
				sign := 1
				if strZone[0] == '-' {
					sign = -1
				}
				return time.FixedZone("fixed", (hour*3600+minute*60)*sign), nil
			}
			return nil, fmt.Errorf("unknown timezone string %s", string(v))
		} else {
			return locObj, nil
		}
	default:
		return nil, fmt.Errorf("timezone must either be number of hours or valid timezone string")
	}
}

func (e Extractor) Apply(_ context.Context, staticKwargs operators.Kwargs, in operators.InputIter, out *value.List) error {
	field := string(staticKwargs.GetUnsafe("field").(value.String))

	lastTimeFormat := time.RFC3339

	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}

		var rawV value.Value
		by, _ := kwargs.Get("on")
		if by == nil {
			rawV = heads[0]
		} else {
			rawV = by
		}

		var t time.Time
		switch rawV := rawV.(type) {
		case value.Int:
			fromTimezoneLoc, err := getTimezone(kwargs.GetUnsafe("timezone"))
			if err != nil {
				return err
			}
			t = time.Unix(int64(rawV), 0).In(fromTimezoneLoc)
		case value.Double:
			fromTimezoneLoc, err := getTimezone(kwargs.GetUnsafe("timezone"))
			if err != nil {
				return err
			}
			t = time.Unix(int64(rawV), 0).In(fromTimezoneLoc)
		case value.String:
			t, err = time.Parse(lastTimeFormat, string(rawV))
			if err != nil {
				for _, timeFormat := range [...]string{time.RFC3339, time.RFC3339Nano, time.UnixDate, time.RFC1123, time.RFC1123Z} {
					t, err = time.Parse(timeFormat, string(rawV))
					if err == nil {
						lastTimeFormat = timeFormat
						break
					}
				}
				if err != nil {
					return fmt.Errorf("unparsable time string of %s", string(rawV))
				}
			}
		default:
			return fmt.Errorf("time field to extract should be a number. Got [%s]", rawV.String())
		}

		timezone := kwargs.GetUnsafe("timezone")
		if timezone != nil {
			timezoneLoc, err := getTimezone(timezone)
			if err != nil {
				return err
			} else {
				t = t.In(timezoneLoc)
			}
		}

		part := string(kwargs.GetUnsafe("part").(value.String))
		var extracted value.Value
		if strings.ToLower(part) == "rfc3339" {
			extracted = value.String(t.Format(time.RFC3339))
		} else if strings.ToLower(part) == "timestamp" {
			extracted = value.Int(t.Unix())
		} else if strings.ToLower(part) == "slot" {
			slotSize := int64(staticKwargs.GetUnsafe("slot_size").(value.Int))
			slotCycle := int64(staticKwargs.GetUnsafe("slot_cycle").(value.Int))
			if slotSize <= 0 || slotCycle <= 1 {
				return fmt.Errorf("slot_size must be positive integer and slot cycle must be > 1 if to extract the slot number")
			}
			extracted = value.Int(t.Unix() / slotSize % slotCycle)
		} else {
			extractor, ok := extractorMap[strings.ToLower(part)]
			if ok {
				extracted = value.Int(extractor(t))
			} else {
				return fmt.Errorf("part to extract can only be in [year,month,day,hour,minute,second,dayofweek,dayofyear,timestamp,rfc3339,slot]")
			}
		}

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
