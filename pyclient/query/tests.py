import unittest
from query.expr import InvalidQueryException, Var, Int
from visitor import Printer

class Test(unittest.TestCase):
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

