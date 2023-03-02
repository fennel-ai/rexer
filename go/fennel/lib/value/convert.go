package value

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var pvalueNilInferred = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "pvalue_nil_inferred",
		Help: "Number of times pvalue with nil node was inferred as value.Nil",
	},
)

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
		pvalueNilInferred.Inc()
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
