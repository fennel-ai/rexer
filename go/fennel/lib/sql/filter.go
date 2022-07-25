package sql

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

type SqlFilter interface {
	fmt.Stringer
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

func (f filterName) Hash() uint64 {
	return hash(string(f))
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

func (f *filterValue) String() string {
	if f.SingleValue != "" {
		return f.SingleValue
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

type simpleSqlFilter struct {
	Name  filterName     `json:"Name"`
	Op    filterOperator `json:"Op"`
	Value *filterValue   `json:"Value"`
	hash  uint64
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
	if f.Hash() != other.Hash() {
		return false
	}
	return f.Name.Equal(o.Name) && f.Op.Equal(o.Op) && f.Value.Equal(o.Value)
}

func (f *simpleSqlFilter) Hash() uint64 {
	if f.hash != 0 {
		return f.hash
	}
	hash := fnv.New64()
	hash.Write([]byte(fmt.Sprintf("%v%v%v", f.Name.Hash(), f.Op.Hash(), f.Value.Hash())))
	f.hash = hash.Sum64()
	return f.hash
}

type compositeSqlFilter struct {
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
		if ret.Left.Hash() < ret.Right.Hash() {
			ret.canonicalLeft = ret.Left
			ret.canonicalRight = ret.Right
		} else {
			ret.canonicalLeft = ret.Right
			ret.canonicalRight = ret.Left
		}
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
	if f.Hash() != oth.Hash() {
		return false
	}
	return f.canonicalLeft.Equal(oth.canonicalLeft) && f.Op.Equal(oth.Op) && f.canonicalRight.Equal(oth.canonicalRight)
}

func (f *compositeSqlFilter) Hash() uint64 {
	if f.hash != 0 {
		return f.hash
	}
	hash := fnv.New64()
	hash.Write([]byte(fmt.Sprintf("%v%v%v", f.canonicalLeft.Hash(), f.Op.Hash(), f.canonicalRight.Hash())))
	f.hash = hash.Sum64()
	return f.hash
}
