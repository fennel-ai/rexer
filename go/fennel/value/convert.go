package value

import (
	"fennel/value/proto"
	"fmt"
)

func ToProtoValue(v Value) (proto.PValue, error) {
	switch v.(type) {
	case Int:
		return proto.PValue{Node: &proto.PValue_Int{Int: int64(v.(Int))}}, nil
	case Double:
		return proto.PValue{Node: &proto.PValue_Double{Double: float64(v.(Double))}}, nil
	case Bool:
		return proto.PValue{Node: &proto.PValue_Bool{Bool: bool(v.(Bool))}}, nil
	case String:
		return proto.PValue{Node: &proto.PValue_String_{String_: string(v.(String))}}, nil
	case List:
		list := make([]*proto.PValue, len(v.(List)))
		for i, v := range v.(List) {
			pv, err := ToProtoValue(v)
			if err != nil {
				return proto.PValue{Node: &proto.PValue_Nil{}}, err
			}
			list[i] = &pv
		}
		pvl := &proto.PVList{Values: list}
		return proto.PValue{Node: &proto.PValue_List{List: pvl}}, nil
	case Dict:
		pvd, err := ToProtoDict(v.(Dict))
		if err != nil {
			return proto.PValue{Node: &proto.PValue_Nil{}}, err
		}
		return proto.PValue{Node: &proto.PValue_Dict{Dict: &pvd}}, nil
	case Table:
		list := make([]*proto.PVDict, 0)
		table := v.(Table)
		for _, v := range table.Pull() {
			pv, err := ToProtoDict(v)
			if err != nil {
				return proto.PValue{Node: &proto.PValue_Nil{}}, err
			}
			list = append(list, &pv)
		}
		pvl := &proto.PVTable{Rows: list}
		return proto.PValue{Node: &proto.PValue_Table{Table: pvl}}, nil
	case nil_:
		return proto.PValue{Node: &proto.PValue_Nil{}}, nil
	default:
		return proto.PValue{Node: &proto.PValue_Nil{}}, fmt.Errorf("invalid value: %v", v)
	}
}

func FromProtoValue(pv *proto.PValue) (Value, error) {
	if pvi, ok := pv.Node.(*proto.PValue_Int); ok {
		return Int(pvi.Int), nil
	}
	if pvd, ok := pv.Node.(*proto.PValue_Double); ok {
		return Double(pvd.Double), nil
	}
	if pvs, ok := pv.Node.(*proto.PValue_String_); ok {
		return String(pvs.String_), nil
	}
	if pvb, ok := pv.Node.(*proto.PValue_Bool); ok {
		return Bool(pvb.Bool), nil
	}
	if pvl, ok := pv.Node.(*proto.PValue_List); ok {
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
	if pvd, ok := pv.Node.(*proto.PValue_Dict); ok {
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
	if pvt, ok := pv.Node.(*proto.PValue_Table); ok {
		ret := NewTable()
		for _, pv := range pvt.Table.Rows {
			d, err := FromProtoDict(pv)
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

	if _, ok := pv.Node.(*proto.PValue_Nil); ok {
		return Nil, nil
	}
	return Nil, fmt.Errorf("unrecognized proto value type: %v", pv.Node)
}

func FromProtoDict(pd *proto.PVDict) (Dict, error) {
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

func ToProtoDict(d Dict) (proto.PVDict, error) {
	dict := make(map[string]*proto.PValue, len(d))
	for k, v := range d {
		pv, err := ToProtoValue(v)
		if err != nil {
			return proto.PVDict{}, err
		}
		dict[k] = &pv
	}
	return proto.PVDict{Values: dict}, nil
}
