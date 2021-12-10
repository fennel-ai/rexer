import unittest
from _expr import Query, At, InvalidQueryException, Var, Table, List, Dict, Int

class Test(unittest.TestCase):
    def test_basic(self):
        with Query() as q:
            a = Int(1, name='a')
            b = Var(name='x')
            c = a + b
            c.setname('c')
            d = a * Int(2)
            d.setname('d')
        s = q.execute(varvalues={b:Int(5)}, printonly=True)
        expected = ['x = 5;', 'a = 1;', 'c = $a + $x;', 'd = $a * 2;']
        self.assertEqual('\n'.join(expected), s)

    def test_basic_inline(self):
        with Query() as q:
            a = Int(1)
            b = Int(2)
            c = a + b
            c.setname('c')
        s = q.execute(printonly=True)
        expected = ['c = 1 + 2;']
        self.assertEqual('\n'.join(expected), s)

    def test_no_conditional(self):
        # verify that it's not possible to use nodes in conditionals
        with self.assertRaises(InvalidQueryException):
            with Query() as q:
                a = Int(1)
                if a == 1:
                    b = Int(2)
                else:
                    b = Int(3)
            q.execute()

        # but it is okay to create conditionals using normal python variables
        with Query() as q:
            a = Int(1)
            pya = 1
            if pya == 1:
                b = Int(2)
            else:
                b = Int(3)
        q.execute()