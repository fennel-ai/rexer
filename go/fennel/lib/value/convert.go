package value

import (
	"fmt"
	"sort"

	"capnproto.org/go/capnp/v3"
)

func ToCapnValue(v Value) (CapnValue, []byte, error) {
	// Make a brand new empty message. A Message allocates Cap'n Proto structs.
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return CapnValue{}, nil, err
	}
	cv, err := NewRootCapnValue(seg)
	if err != nil {
		return CapnValue{}, nil, err
	}
	switch t := v.(type) {
	case Int:
		cv.SetInt(int64(t))
	case Double:
		cv.SetDouble(float64(t))
	case Bool:
		cv.SetBool(bool(t))
	case String:
		err = cv.SetStr(string(t))
		if err != nil {
			return CapnValue{}, nil, err
		}
	case List:
		l, err := cv.NewList(int32(t.Len()))
		if err != nil {
			return CapnValue{}, nil, err
		}
		for i, v := range t.values {
			cv, _, err := ToCapnValue(v)
			if err != nil {
				return CapnValue{}, nil, err
			}
			err = l.Set(i, cv)
			if err != nil {
				return CapnValue{}, nil, err
			}
		}
	case Dict:
		m, err := cv.NewDict()
		if err != nil {
			return cv, nil, err
		}
		entries := t.Iter()
		es, err := m.NewEntries(int32(len(entries)))
		if err != nil {
			return cv, nil, err
		}
		// Collect keys in dict and sort to get canonical order.
		keys := make([]string, 0, len(entries))
		for k := range entries {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			key, err := capnp.NewText(seg, k)
			if err != nil {
				return CapnValue{}, nil, fmt.Errorf("failed to convert map key to string: %w", err)
			}
			err = es.At(i).SetKey(key.ToPtr())
			if err != nil {
				return CapnValue{}, nil, fmt.Errorf("failed to set map key: %w", err)
			}
			value, _, err := ToCapnValue(entries[k])
			if err != nil {
				return CapnValue{}, nil, err
			}
			err = es.At(i).SetValue(value.ToPtr())
			if err != nil {
				return CapnValue{}, nil, fmt.Errorf("failed to set map value: %w", err)
			}
		}
	case nil_:
		cv.SetNil()
	default:
		return CapnValue{}, nil, fmt.Errorf("invalid value: %s", v.String())
	}
	serializedProto, err := msg.Marshal()
	return cv, serializedProto, err
}

func FromCapnValue(cv CapnValue) (Value, error) {
	switch cv.Which() {
	case CapnValue_Which_nil:
		return Nil, nil
	case CapnValue_Which_int:
		return Int(cv.Int()), nil
	case CapnValue_Which_double:
		return Double(cv.Double()), nil
	case CapnValue_Which_bool:
		return Bool(cv.Bool()), nil
	case CapnValue_Which_str:
		s, err := cv.Str()
		if err != nil {
			return Nil, fmt.Errorf("capnvalue is not string: %v", cv.String())
		}
		return String(s), nil
	case CapnValue_Which_list:
		l, err := cv.List()
		if err != nil {
			return Nil, fmt.Errorf("capnvalue is not list: %v", cv.String())
		}
		v := NewList()
		v.Grow(l.Len())
		for i := 0; i < l.Len(); i++ {
			e := l.At(i)
			ve, err := FromCapnValue(e)
			if err != nil {
				return Nil, err
			}
			v.Append(ve)
		}
		return v, nil
	case CapnValue_Which_dict:
		d, err := cv.Dict()
		if err != nil {
			return Nil, fmt.Errorf("capnvalue is not dict: %v", cv.String())
		}
		v := NewDict(make(map[string]Value))
		entries, err := d.Entries()
		if err != nil {
			return Nil, fmt.Errorf("failed to get dict entries from capnvalue: %w", err)
		}
		for i := 0; i < entries.Len(); i++ {
			entry := entries.At(i)
			ek, err := entry.Key()
			if err != nil {
				return Nil, fmt.Errorf("failed to get dict key from capnvalue: %w", err)
			}
			ev, err := entry.Value()
			if err != nil {
				return Nil, fmt.Errorf("failed to get dict value from capnvalue: %w", err)
			}
			val, err := FromCapnValue(CapnValue{ev.Struct()})
			if err != nil {
				return Nil, fmt.Errorf("failed to convert dict value from capnvalue to Value: %w", err)
			}
			v.Set(ek.Text(), val)
		}
		return v, nil
	default:
		return Nil, fmt.Errorf("invalid value: %v", cv.String())
	}
}

func ToProtoValue(v Value) (PValue, error) {
	switch t := v.(type) {
	case Int:
		return PValue{Node: &PValue_Int{Int: int64(t)}}, nil
	case Double:
		return PValue{Node: &PValue_Double{Double: float64(t)}}, nil
	case Bool:
		return PValue{Node: &PValue_Bool{Bool: bool(t)}}, nil
	case String:
		return PValue{Node: &PValue_String_{String_: string(t)}}, nil
	case List:
		list := make([]*PValue, t.Len())
		for i, v := range t.values {
			pv, err := ToProtoValue(v)
			if err != nil {
				return PValue{Node: &PValue_Nil{}}, err
			}
			list[i] = &pv
		}
		pvl := &PVList{Values: list}
		return PValue{Node: &PValue_List{List: pvl}}, nil
	case Dict:
		pvd, err := ToProtoDict(t)
		if err != nil {
			return PValue{Node: &PValue_Nil{}}, err
		}
		return PValue{Node: &PValue_Dict{Dict: &pvd}}, nil
	case nil_:
		return PValue{Node: &PValue_Nil{}}, nil
	default:
		return PValue{Node: &PValue_Nil{}}, fmt.Errorf("invalid value: %v", v)
	}
}

func FromProtoValue(pv *PValue) (Value, error) {
	if pvi, ok := pv.Node.(*PValue_Int); ok {
		return Int(pvi.Int), nil
	}
	if pvd, ok := pv.Node.(*PValue_Double); ok {
		return Double(pvd.Double), nil
	}
	if pvs, ok := pv.Node.(*PValue_String_); ok {
		return String(pvs.String_), nil
	}
	if pvb, ok := pv.Node.(*PValue_Bool); ok {
		return Bool(pvb.Bool), nil
	}
	if pvl, ok := pv.Node.(*PValue_List); ok {
		ret := make([]Value, len(pvl.List.Values))
		for i, pv := range pvl.List.Values {
			v, err := FromProtoValue(pv)
			if err != nil {
				return Nil, fmt.Errorf("can not convert element of list to value: %v", pv)
			}
			ret[i] = v
		}
		return NewList(ret...), nil
	}
	if pvd, ok := pv.Node.(*PValue_Dict); ok {
		ret := make(map[string]Value, len(pvd.Dict.Values))
		for k, pv := range pvd.Dict.Values {
			v, err := FromProtoValue(pv)
			if err != nil {
				return Nil, fmt.Errorf("can not convert element of dict to value: %v", pv)
			}
			ret[k] = v
		}
		return NewDict(ret), nil
	}
	if _, ok := pv.Node.(*PValue_Nil); ok {
		return Nil, nil
	}

	// TODO(mohit): Remove this hack once Vitess supports consistent marshaling and unmarshaling support as
	// regular proto
	//
	// See -
	// 	https://github.com/planetscale/vtprotobuf/issues/60
	// 	https://github.com/planetscale/vtprotobuf/issues/61
	if pv.Node == nil {
		return Nil, nil
	}

	return Nil, fmt.Errorf("unrecognized proto value type: %v", pv.Node)
}

func FromProtoDict(pd *PVDict) (Dict, error) {
	ret := make(map[string]Value, 0)
	for k, pv := range pd.Values {
		v, err := FromProtoValue(pv)
		if err != nil {
			return Dict{}, fmt.Errorf("can not convert element of dict to value: %v", pv)
		}
		ret[k] = v
	}
	return NewDict(ret), nil
}

func ToProtoDict(d Dict) (PVDict, error) {
	dict := make(map[string]*PValue, d.Len())
	for k, v := range d.Iter() {
		pv, err := ToProtoValue(v)
		if err != nil {
			return PVDict{}, err
		}
		dict[k] = &pv
	}
	return PVDict{Values: dict}, nil
}
