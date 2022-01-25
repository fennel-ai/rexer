import unittest
from query.expr import *
from query.visitor import Printer
from query.to_proto import ProtoConvertor
from gen.ast_pb2 import Ast


class Test(unittest.TestCase):
    def test_lookup(self):
        a = Int(1, name='a')
        d = Dict(x=a)
        b = d.x
        b.name = 'b'
        printer = Printer()
        expected = '\n'.join(['a = 1;', 'b = {x=$a}.x;', '$b'])
        self.assertEqual(expected, printer.print(b))

    def test_basic_noinline(self):
        a = Int(1, name='a')
        b = Var(name='b')
        c = a + b
        c.name = 'c'
        printer = Printer()
        expected = '\n'.join(['b = 5;', 'a = 1;', 'c = $a + $b;', '$c'])
        self.assertEqual(expected, printer.print(c, varvalues={b: Int(5)}))

    def test_basic_inline(self):
        a = Int(1)
        b = Var(name='b')
        c = a + b
        printer = Printer()
        expected = '\n'.join(['b = 5;', '1 + $b'])
        self.assertEqual(expected, printer.print(c, varvalues={b: Int(5)}))

    def test_no_conditional(self):
        # verify that it's not possible to use nodes in conditionals
        with self.assertRaises(InvalidQueryException):
            a = Int(1)
            if a == 1:
                b = Int(2)
            else:
                b = Int(3)
            Printer().print(b)

        # but it is okay to create conditionals using normal python variables
        a = Int(1)
        pya = 1
        if pya == 1:
            b = Int(2)
        else:
            b = Int(3)
        Printer().print(b)

    def test_expr_to_ast(self):
        tests = []
        x = Int(1)
        y = Int(2)
        tests.append(x + y)

        x = 5
        y = Dict(hello=Int(x), bye=Bool(False))
        tests.append(y)

        z = Var('inputs').uid
        tests.append(z)

        l = List(Int(5), y, z)
        tests.append(l)

        d1 = Dict(x=Int(1), y=Int(2))
        d2 = Dict(x=Int(3), y=Int(4))
        t = Table(List(d1, d2))
        tests.append(t)
        e = Transform(t).using(Ops.std.filter(where=at.x + at.y < Int(4)))
        tests.append(e)
        for t in tests:
            q = ProtoConvertor().query(t)
            s = q.SerializeToString()
            q2 = Ast()
            q2.ParseFromString(s)
            self.assertEqual(q, q2)
