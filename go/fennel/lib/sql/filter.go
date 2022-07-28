package sql

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strings"
)

type SqlFilter interface {
	fmt.Stringer
	json.Marshaler
	Equal(SqlFilter) bool
	Hash() uint64
}

func hash(str string) uint64 {
	hash := fnv.New64a()
	hash.Write([]byte(str))
	return hash.Sum64()
}

type filterOperator string

func (f filterOperator) String() string {
	return string(f)
}

func (f filterOperator) Hash() uint64 {
	return hash(string(f))
}

func (f filterOperator) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.String())
}

func (f filterOperator) Equal(other SqlFilter) bool {
	o, ok := other.(filterOperator)
	if !ok {
		return false
	}
	if f.Hash() != o.Hash() {
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
	_ SqlFilter        = (*CompositeSqlFilter)(nil)
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

func (f filterName) Hash() uint64 {
	return hash(string(f))
}

func (f filterName) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.String())
}

type filterValue struct {
	SingleValue string
	MultiValue  []string
	// Sorted order to have consistent ordering in case of MultiValue.
	canonicalMultiValue []string
	hash                uint64
}

func (f *filterValue) UnmarshalJSON(data []byte) error {
	var d string
	var err error
	if err = json.Unmarshal(data, &d); err != nil {
		var da []string
		err = json.Unmarshal(data, &da)
		if err == nil {
			f.MultiValue = da
			f.canonicalMultiValue = make([]string, len(f.MultiValue))
			sort.Strings([]string(f.canonicalMultiValue))
			return nil
		}
		return err
	}
	f.SingleValue = d
	f.hash = hash(f.String())
	return nil
}

func (f *filterValue) MarshalJSON() ([]byte, error) {
	if len(f.SingleValue) > 0 {
		return json.Marshal(f.SingleValue)
	} else if len(f.MultiValue) > 0 {
		return json.Marshal(f.MultiValue)
	}
	return nil, fmt.Errorf("filterValue should be either be SingleValue or MultiValue")
}

func (f *filterValue) String() string {
	if f.SingleValue != "" {
		return `"` + f.SingleValue + `"`
	}
	if f.MultiValue != nil {
		return "(" + strings.Join(f.canonicalMultiValue, ",") + ")"
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

func (f *filterValue) Hash() uint64 {
	if f.hash != 0 {
		f.hash = hash(f.String())
		return f.hash
	}
	return f.hash
}

type CompositeSqlFilter struct {
	Left           SqlFilter
	canonicalLeft  SqlFilter
	Op             filterOperator
	Right          SqlFilter
	canonicalRight SqlFilter
	hash           uint64
}

func containsKeys(m map[string]interface{}, keys []string) bool {
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}

func fromMap(m map[string]any, ret *CompositeSqlFilter) error {
	keys := []string{"Left", "Op", "Right"}
	if containsKeys(m, keys) {
		for _, k := range keys {
			switch t := m[k].(type) {
			case string:
				switch k {
				case "Left":
					ret.Left = filterName(t)
				case "Op":
					ret.Op = filterOperator(t)
				case "Right":
					ret.Right = &filterValue{
						SingleValue: t,
					}
				default:
					return fmt.Errorf("unpexpected key in json: %s", k)
				}
			case []any:
				switch k {
				case "Right":
					vals := make([]string, len(t))
					var ok bool
					for i := range vals {
						if vals[i], ok = t[i].(string); !ok {
							return fmt.Errorf("expected []string type in %s expression, got %v", k, t)
						}

					}

					ret.Right = &filterValue{
						MultiValue: vals,
					}
				default:
					return fmt.Errorf("expected []string type in %s expression", k)
				}
			case map[string]any:
				switch k {
				case "Left":
					ret.Left = new(CompositeSqlFilter)
					var err error
					if err = fromMap(t, ret.Left.(*CompositeSqlFilter)); err != nil {
						return err
					}
				case "Right":
					ret.Right = new(CompositeSqlFilter)
					var err error
					if err = fromMap(t, ret.Right.(*CompositeSqlFilter)); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("unpexpected value %s in %s expression", reflect.TypeOf(m[k]), k)
			}
		}
		ret.canonicalLeft = ret.Left
		ret.canonicalRight = ret.Right
		if ret.Left.Hash() > ret.Right.Hash() {
			ret.canonicalLeft = ret.Right
			ret.canonicalRight = ret.Left
		}
		return nil
	}
	return fmt.Errorf("unexpected sub json structure in filter expression, %v", m)
}

func (f *CompositeSqlFilter) String() string {
	return fmt.Sprintf("(%s %s %s)", f.Left.String(), f.Op, f.Right.String())
}

func (f *CompositeSqlFilter) MarshalJSON() ([]byte, error) {
	lb, err := json.Marshal(f.Left)
	if err != nil {
		return nil, err
	}
	op, err := json.Marshal(f.Op)
	if err != nil {
		return nil, err
	}
	rb, err := json.Marshal(f.Right)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf(`
			{
				"Left": %s,
				"Op": %s,
				"Right": %s
			}
	`, lb, op, rb)), nil
}

func (f *CompositeSqlFilter) Equal(other SqlFilter) bool {
	oth, ok := other.(*CompositeSqlFilter)
	if !ok {
		return false
	}
	if f.Hash() != oth.Hash() {
		return false
	}
	return f.canonicalLeft.Equal(oth.canonicalLeft) && f.Op.Equal(oth.Op) && f.canonicalRight.Equal(oth.canonicalRight)
}

func (f *CompositeSqlFilter) Hash() uint64 {
	if f.hash != 0 {
		return f.hash
	}
	hash := fnv.New64()
	hash.Write([]byte(fmt.Sprintf("%v%v%v", f.canonicalLeft.Hash(), f.Op.Hash(), f.canonicalRight.Hash())))
	f.hash = hash.Sum64()
	return f.hash
}

func (f *CompositeSqlFilter) UnmarshalJSON(b []byte) error {
	var d map[string]any
	if err := json.Unmarshal(b, &d); err != nil {
		return err
	}
	if err := fromMap(d, f); err != nil {
		return err
	}
	return nil
}
