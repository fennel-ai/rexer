package sql

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type SqlFilter interface {
	fmt.Stringer
	Equal(SqlFilter) bool
}

type filterOperator string

func (f filterOperator) String() string {
	return string(f)
}

func (f filterOperator) Equal(other SqlFilter) bool {
	o, ok := other.(filterOperator)
	if !ok {
		return false
	}
	return f.String() == o.String()
}

const (
	EQUAL     filterOperator = "="
	NOT_EQUAL filterOperator = "!="
	IN        filterOperator = "in"
	AND       filterOperator = "and"
	OR        filterOperator = "or"
)

var (
	_ json.Unmarshaler = (*filterValue)(nil)
	_ SqlFilter        = (*filterName)(nil)
	_ SqlFilter        = (*filterValue)(nil)
	_ SqlFilter        = (*filterOperator)(nil)
	_ SqlFilter        = (*simpleSqlFilter)(nil)
	_ SqlFilter        = (*compositeSqlFilter)(nil)
)

type filterName string

func (f filterName) String() string {
	return string(f)
}

func (f filterName) Equal(other SqlFilter) bool {
	o, ok := other.(filterName)
	if !ok {
		return false
	}
	return f.String() == o.String()
}

type filterValue struct {
	SingleValue string
	MultiValue  []string
}

func (f *filterValue) UnmarshalJSON(data []byte) error {
	var d string
	var err error
	if err = json.Unmarshal(data, &d); err != nil {
		var da []string
		err = json.Unmarshal(data, &da)
		if err == nil {
			f.MultiValue = da
			sort.Strings([]string(f.MultiValue))
			return nil
		}
		return err
	}
	f.SingleValue = d
	return nil
}

func (f *filterValue) String() string {
	if f.SingleValue != "" {
		return f.SingleValue
	}
	if f.MultiValue != nil {

		return "(" + strings.Join(f.MultiValue, ",") + ")"
	}
	return ""
}

func (f *filterValue) Equal(other SqlFilter) bool {
	o, ok := other.(*filterValue)
	if !ok {
		return false
	}
	return f.String() == o.String()
}

type simpleSqlFilter struct {
	Name  filterName     `json:"Name"`
	Op    filterOperator `json:"Op"`
	Value *filterValue   `json:"Value"`
}

func (f *simpleSqlFilter) String() string {
	defaultResult := "1 = 1"

	switch f.Op {
	case EQUAL, NOT_EQUAL, IN:
		return fmt.Sprintf("(%s %s \"%s\")", f.Name, f.Op, f.Value)
	}
	return defaultResult
}

func (f *simpleSqlFilter) Equal(other SqlFilter) bool {
	o, ok := other.(*simpleSqlFilter)
	if !ok {
		return false
	}
	return f.Name.Equal(o.Name) && f.Op.Equal(o.Op) && f.Value.Equal(o.Value)
}

type compositeSqlFilter struct {
	Left  SqlFilter
	Op    filterOperator
	Right SqlFilter
}

func containsKeys(m map[string]interface{}, keys []string) bool {
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}

func FromJSON(data []byte) (SqlFilter, error) {
	var d map[string]any
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, err
	}
	if containsKeys(d, []string{"Name", "Op", "Value"}) {
		ret := new(simpleSqlFilter)
		err := json.Unmarshal(data, &ret)
		return ret, err
	}
	if containsKeys(d, []string{"Left", "Op", "Right"}) {
		ret, err := fromMap(d)
		return ret, err
	}
	return nil, fmt.Errorf("unexpected expression in filter json: %s", string(data))
}

func fromMap(m map[string]any) (SqlFilter, error) {
	if containsKeys(m, []string{"Name", "Op", "Value"}) {
		data, err := json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("unexpected failure in marshaling map to json")
		}
		return FromJSON(data)
	}
	if containsKeys(m, []string{"Left", "Op", "Right"}) {
		ret := new(compositeSqlFilter)
		var v map[string]any
		var ok bool
		var err error
		if v, ok = m["Left"].(map[string]any); !ok {
			return nil, fmt.Errorf("unexpected value of \"Left\" in composite filter, expected struct, got %v", m["Left"])
		}
		ret.Left, err = fromMap(v)
		if err != nil {
			return nil, err
		}

		var x string
		if x, ok = m["Op"].(string); !ok {
			return nil, fmt.Errorf("unexpected value of \"Operator\" in composite filter, expected string, got %v", m["Op"])
		}
		ret.Op = filterOperator(x)
		if v, ok = m["Right"].(map[string]any); !ok {
			return nil, fmt.Errorf("unexpected value of \"Value\" in composite filter, expected struct, got %v", m["Right"])
		}
		ret.Right, err = fromMap(v)
		return ret, err

	}
	return nil, fmt.Errorf("unexpected sub json structure in filter expression, %v", m)
}

func (f *compositeSqlFilter) String() string {
	return fmt.Sprintf("(%s %s %s)", f.Left.String(), f.Op, f.Right.String())
}

func (f *compositeSqlFilter) Equal(other SqlFilter) bool {
	oth, ok := other.(*compositeSqlFilter)
	if !ok {
		return false
	}
	// composite filter trees could be mirror images as and and or are commutative.
	// Hence, checking both left == left as well as left == right.
	if !f.Left.Equal(oth.Left) && !f.Left.Equal(oth.Right) {
		return false
	}

	if !f.Right.Equal(oth.Right) && !f.Right.Equal(oth.Left) {
		return false
	}
	return true
}
