from query.expr import Int, Double, Bool, String
from query.expr import Expr as _Expr
from query.expr import InvalidQueryException, List, Dict, Transform, Table, Var, Ops, at
from query.visitor import Printer
from query.to_proto import ProtoConvertor as _ToQueryProto
from query.to_proto import proto as Query


def query(e: _Expr):
    return _ToQueryProto().query(e)


if __name__ == '__main__':
    a = Int(1, name='a')
    b = Int(8)
    c = a + b
    x = Var()
    l = List(a, c, Int(5), x)
    res = Transform(l).using(
        Ops.std.filter(where=at.x > at.y + Int(1)),
        Ops.std.take(count=Int(1)),
        Ops.std.take(where=at.z == a),
    )
    x.name = 'x'

    printer = Printer(varvalues={x: Int(3)})
    print(printer.print(res))
