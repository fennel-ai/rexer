package value

import (
	"fmt"
)

func ToProtoValue(v Value) (PValue, error) {
	switch v.(type) {
	case Int:
		return PValue{Node: &PValue_Int{Int: int64(v.(Int))}}, nil
	case Double:
		return PValue{Node: &PValue_Double{Double: float64(v.(Double))}}, nil
	case Bool:
		return PValue{Node: &PValue_Bool{Bool: bool(v.(Bool))}}, nil
	case String:
		return PValue{Node: &PValue_String_{String_: string(v.(String))}}, nil
	case List:
		list := make([]*PValue, len(v.(List)))
		for i, v := range v.(List) {
			pv, err := ToProtoValue(v)
			if err != nil {
				return PValue{Node: &PValue_Nil{}}, err
			}
			list[i] = &pv
		}
		pvl := &PVList{Values: list}
		return PValue{Node: &PValue_List{List: pvl}}, nil
	case Dict:
		pvd, err := toProtoDict(v.(Dict))
		if err != nil {
			return PValue{Node: &PValue_Nil{}}, err
		}
		return PValue{Node: &PValue_Dict{Dict: &pvd}}, nil
	case Table:
		list := make([]*PVDict, 0)
		table := v.(Table)
		for _, v := range table.Pull() {
			pv, err := toProtoDict(v)
			if err != nil {
				return PValue{Node: &PValue_Nil{}}, err
			}
			list = append(list, &pv)
		}
		pvl := &PVTable{Rows: list}
		return PValue{Node: &PValue_Table{Table: pvl}}, nil
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
		ret := make([]Value, 0)
		for _, pv := range pvl.List.Values {
			v, err := FromProtoValue(pv)
			if err != nil {
				return Nil, fmt.Errorf("can not convert element of list to value: %v", pv)
			}
			ret = append(ret, v)
		}
		return List(ret), nil
	}
	if pvd, ok := pv.Node.(*PValue_Dict); ok {
		ret := make(map[string]Value, 0)
		for k, pv := range pvd.Dict.Values {
			v, err := FromProtoValue(pv)
			if err != nil {
				return Nil, fmt.Errorf("can not convert element of dict to value: %v", pv)
			}
			ret[k] = v
		}
		return NewDict(ret)
	}
	if pvt, ok := pv.Node.(*PValue_Table); ok {
		ret := NewTable()
		for _, pv := range pvt.Table.Rows {
			d, err := fromProtoDict(pv)
			if err != nil {
				return Nil, fmt.Errorf("can not convert element of dict to value: %v", pv)
			}
			err = ret.Append(d)
			if err != nil {
				return Nil, fmt.Errorf("can not append dict: %v", err)
			}
		}
		return ret, nil
	}

	if _, ok := pv.Node.(*PValue_Nil); ok {
		return Nil, nil
	}
	return Nil, fmt.Errorf("unrecognized proto value type: %v", pv.Node)
}

func fromProtoDict(pd *PVDict) (Dict, error) {
	ret := make(map[string]Value, 0)
	for k, pv := range pd.Values {
		v, err := FromProtoValue(pv)
		if err != nil {
			return Dict{}, fmt.Errorf("can not convert element of dict to value: %v", pv)
		}
		ret[k] = v
	}
	return ret, nil
}

func toProtoDict(d Dict) (PVDict, error) {
	dict := make(map[string]*PValue, len(d))
	for k, v := range d {
		pv, err := ToProtoValue(v)
		if err != nil {
			return PVDict{}, err
		}
		dict[k] = &pv
	}
	return PVDict{Values: dict}, nil
}
