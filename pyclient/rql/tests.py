import unittest
from rql.expr import *
from rql.visitor import Printer
from rql.serializer import Serializer
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
            q = Serializer().serialize(t)
            s = q.SerializeToString()
            q2 = Ast()
            q2.ParseFromString(s)
            self.assertEqual(q, q2)

    def test_naked_opcalls(self):
        x = Ops.std.filter(where=at.x > Int(1))
        printer = Printer()
        with self.assertRaises(InvalidQueryException):
            printer.print(x)
        q = Serializer()
        with self.assertRaises(InvalidQueryException):
            q.serialize(x)

    def test_var(self):
        x = Var('args').actions
        q = Serializer()
        expected = Ast()
        var = Ast()
        var.var.name = 'args'
        expected.lookup.on.CopyFrom(var)
        expected.lookup.property = 'actions'
        self.assertEqual(expected, q.visit(x))
