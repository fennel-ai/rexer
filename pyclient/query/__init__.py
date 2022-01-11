from query.expr import Int, Double, Bool, String
from query.expr import InvalidQueryException, List, Dict, Transform, Table, Var, Ops, at
from query.visitor import  Printer


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