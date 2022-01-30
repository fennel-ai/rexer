from typing import List, Dict

from gen.value_pb2 import PValue as Value
from gen.value_pb2 import PVNil as _PVNil


class InvalidValue(Exception):
    pass


def Int(n: int) -> Value:
    v = Value()
    v.Int = n
    return v


def Double(d: float) -> Value:
    v = Value()
    v.Double = d
    return v


def String(s: str) -> Value:
    v = Value()
    v.String = s
    return v


def Bool(b: bool) -> Value:
    v = Value()
    v.Bool = b
    return v


def List(*l: List[Value]) -> Value:
    v = Value()
    for e in l:
        if not is_valid(e):
            raise InvalidValue("%e in argument l is not valid value" % e)
    v.List.values.extend(l)
    return v


def Dict(**d: Dict[str, Value]) -> Value:
    ret = Value()
    # calling this clear now so that at least ret.Dict field is setup
    ret.Dict.SetInParent()
    for k, v in d.items():
        if not isinstance(k, str) or (not is_valid(v)):
            raise InvalidValue()
        ret.Dict.values[k].CopyFrom(v)
    return ret


def Table(*rows) -> Value:
    v = Value()
    # calling this clear now so that at least ret.Table field is setup
    v.Table.SetInParent()
    schema = None
    for row in rows:
        if not is_valid(row):
            raise InvalidValue("row not initialized")
        if not row.HasField("Dict"):
            raise InvalidValue("rows should be list of Dicts")
        its_schema = sorted(row.Dict.values.keys())
        if (schema is not None) and (schema != its_schema):
            raise InvalidValue("table rows should all have the same schema")
        schema = its_schema
        v.Table.rows.append(row.Dict)
    return v


def Nil() -> Value:
    v = Value()
    v.Nil.CopyFrom(_PVNil())
    return v


def is_valid(v: Value) -> bool:
    if not isinstance(v, Value) or not v.IsInitialized():
        return False
    if v.WhichOneof("node") is None:
        return False
    return True
