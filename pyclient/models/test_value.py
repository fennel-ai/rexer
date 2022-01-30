import unittest
from models.value import *


class TestValue(unittest.TestCase):
    def test_basic(self):
        tests = [
            Int(1),
            Int(-1),
            Int(1231231),
            Double(0),
            Double(1.412),
            Double(-1231.3),
            Double(-1e12),
            Bool(True),
            Bool(False),
            String("hi"),
            String("bye"),
            String(""),
            # lists
            List(),
            List(Int(1), Double(2.3), Bool(False)),
            List(Int(1), Double(2.3), Bool(False), List()),
            List(Int(1), Double(2.3), Bool(False), List(Int(2))),
            # dicts
            Dict(),
            Dict(
                x=Int(1),
                y=Bool(False),
            ),
            Dict(x=Dict(x=Int(1)), z=List(Int(2))),
            # tables
            Table(),
            Table(Dict(x=Int(1)), Dict(x=Int(2))),
            # Note: we don't do any type enforcement on field types so this works
            Table(Dict(x=Int(1), y=Bool(False)), Dict(x=Int(2), y=String("hi"))),
            # Nil
            Nil(),
        ]
        for v in tests:
            self.assertTrue(is_valid(v), v)
            self.check_serde(v)

    def test_invalid(self):
        invalids = [
            (Int, ["hi", 3.0, False, List(), Dict(), Table(), Nil()]),
            (Bool, ["hi", 1, 3.0, List(), Dict(), Table(), Nil()]),
            (String, [False, 1, 3.0, List(), Dict(), Table(), Nil()]),
            (Double, ["hi", False, List(), Dict(), Table(), Nil()]),
        ]

        for cls, v in invalids:
            with self.assertRaises(TypeError):
                cls(v)

    def check_serde(self, v):
        ser = v.SerializeToString()
        v2 = Value()
        v2.ParseFromString(ser)
        self.assertEqual(v, v2)


def make_value(n):
    v = Value()
    v.Int = n
    return v


if __name__ == "__main__":
    unittest.main()
