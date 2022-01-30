from __future__ import annotations

from munch import Munch
import random


# DESIGN:
# =======
# Every expression is a sublcass of base type called Expr. We dynamically create
# a graph of these Expr nodes. We do this by overriding +, -, and all other such
# operations on Expr class. This way, when we write e1 + e2, Python calls add
# method on e1, which we intercept and use that point to insert a new 'Add' node
# in the graph.
#
# Every Expr object has a nodeid which is generated once when the object is created
# and does not change after that. In addition, nodes can also be a given an optional
# name - if none is given a random hash is generated. The mere purpose of this name
# is that if we have to create a variable corresponding to the expression of this
# node, that variable will be given the name.
#
# Later when we try to print the query, we traverse this expression graph. And
# if an expr object appears in exactly one more expression, we just inline it
# instead of printing a special line for the expression. Else, we print a line
# assigning this expression to a variable and use this variable wherever else
# this expression was needed.
#
#
# TODOs:
# ======
# - How do people compose the query spread over multiple function calls?
# - Somehow auto generate module/name (and ideally kwarg info) of operators
#   based on existing operators
# - Ensure that we never get to a place of printing @ outside of opcall
# - After the query executes, populate 'result' field on every node


class InvalidQueryException(Exception):
    pass


class Expr(object):
    def __init__(self, name=None):
        self.name = name
        self.nodeid = "%08x" % random.getrandbits(32)
        self.inline = False
        self.out_edges = []

    def edge(self, node):
        self.out_edges.append(node)

    def __getattr__(self, k: str):
        if not isinstance(k, str):
            raise InvalidQueryException(
                "property lookup using '.' can only be done via constant strings but got '%s'" % type(k))
        return Lookup(self, k)

    def __getitem__(self, item):
        if not isinstance(item, Expr):
            raise InvalidQueryException("'[]' operation can only take RQL expression but got '%s'" % type(item))
        return Binary(self, '[]', item)

    def __nonzero__(self):
        raise InvalidQueryException("can not convert: '%s' which is part of query graph to bool" % self)

    def __bool__(self):
        raise InvalidQueryException("can not convert: '%s' which is part of query graph to bool" % self)

    def __add__(self, other: Expr) -> Expr:
        if not isinstance(other, Expr):
            raise InvalidQueryException("'+' only allowed between RQL expressions but got: '%s' instead" % other)
        return Binary(self, '+', other)

    def __or__(self, other: Expr) -> Expr:
        if not isinstance(other, Expr):
            raise InvalidQueryException("'or' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, 'or', other)

    def __eq__(self, other: Expr) -> Expr:
        if not isinstance(other, Expr):
            raise InvalidQueryException("'==' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, '==', other)

    def __ne__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'!=' only allowed between two RQL expressions but got: '%s' instead" % other)
        return not (self == other)

    def __ge__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'>=' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, '>=', other)

    def __gt__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'>' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, '>', other)

    def __sub__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'-' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, '-', other)

    def __mul__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'*' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, '*', other)

    def __truediv__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'/' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, '/', other)

    def __mod__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'%' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, '%', other)

    def __and__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'and' only allowed between two RQL expressions but got: '%s' instead" % other)
        return Binary(self, 'and', other)

    def __xor__(self, other):
        raise InvalidQueryException("binary operation 'xor' not supported by RQL")

    def varname(self, dollar=True):
        name = self.nodeid if self.name is None else self.name
        if dollar:
            return '$%s' % name
        else:
            return name

    def __hash__(self):
        return hash(self.nodeid)

    def num_out_edges(self):
        return len(self.out_edges)


# Concrete Expr classes below
# =============================
# These classes have just to take care of two things:
#   1) They are initialized with valid inputs
#   2) Create right edges in the graph by calling 'edge' function
#
# All other functionality is either abstracted in base Expr class or
# implemented as part of various visitor interfaces.


class _Constant(Expr):
    def __init__(self, c, name=None):
        super(_Constant, self).__init__(name=name)
        self.c = c


class Int(_Constant):
    def __init__(self, n, name=None):
        if not isinstance(n, int):
            raise InvalidQueryException("Int can only be initialized by int, but given '%s'" % n)
        super(Int, self).__init__(n, name=name)


class Double(_Constant):
    def __init__(self, d, name=None):
        if not isinstance(d, float):
            raise InvalidQueryException("Double can only be initialized by float, but given '%s'" % d)
        super(Double, self).__init__(d, name=name)


class Bool(_Constant):
    def __init__(self, b, name=None):
        if not isinstance(b, bool):
            raise InvalidQueryException("Bool can only be initialized by bool, but given '%s'" % b)
        super(Bool, self).__init__(b, name=name)


class String(_Constant):
    def __init__(self, s, name=None):
        if not isinstance(s, str):
            raise InvalidQueryException("String can only be initialized by str, but given '%s'" % s)
        super(String, self).__init__(s, name=name)


class Binary(Expr):
    def __init__(self, left, op, right):
        valid = ('+', '-', '*', '/', '%', 'and', 'or', '==', '>=', '>', '<', '<=', '!=')
        if op not in valid:
            raise InvalidQueryException("RQL binary expressions only supports %s but given '%s'" % (', '.join(valid), op))
        super(Binary, self).__init__()
        self.left = left
        self.op = op
        self.right = right
        left.edge(self)
        right.edge(self)


class Lookup(Expr):
    def __init__(self, on, property):
        if not isinstance(property, str):
            raise InvalidQueryException("for '.' lookup, property can only be string, but given '%s'" % property)
        super(Lookup, self).__init__()
        self.on = on
        self.property = property
        on.edge(self)


class List(Expr):
    def __init__(self, *values, name=None):
        super(List, self).__init__(name=name)
        for i, v in enumerate(values):
            if not isinstance(v, Expr):
                raise InvalidQueryException('list can only contain RQL expressions but got %s at index %d instead' % (v, i))
            v.edge(self)
        self.values = values


class Dict(Expr):
    def __init__(self, name=None, **values):
        super(Dict, self).__init__(name=name)
        self.kwargs = {}
        for k, v in values.items():
            if not isinstance(k, str):
                raise InvalidQueryException('dict keys can only be strings but got %s instead' % k)
            if not isinstance(v, Expr):
                raise InvalidQueryException("dict values can only be RQL expressions but got '%s' for key: %s instead" % (v, k))
            self.kwargs[k] = v
            v.edge(self)


class Table(Expr):
    def __init__(self, inner, name=None):
        super(Table, self).__init__(name=name)
        if not isinstance(inner, Expr):
            raise InvalidQueryException('table can only be created from RQL expressions but got %s instead' % inner)
        self.inner = inner
        inner.edge(self)


class Var(Expr):
    def __init__(self, name=None):
        super(Var, self).__init__(name=name)


class _Opcall(object):
    module = None
    opname = None

    def __init__(self, name=None, **kwargs):
        if (self.module is None) or (self.opname is None):
            raise InvalidQueryException("operator '%s.%s' is not registered" % (self.module, self.opname))

        self.name = name
        self.kwargs = {}
        for k, v in kwargs.items():
            if not isinstance(k, str):
                raise InvalidQueryException("operator argument keys can only be strings but received '%s' instead" % k)
            if not isinstance(v, Expr):
                raise InvalidQueryException(
                    "value for operator parameter '%s' given '%s' but expected a RQL expression" % (k, v))
            self.kwargs[k] = v


Ops = Munch()
Ops.std = Munch({
    'filter': type('Filter', (_Opcall,), {'module': 'std', 'opname': 'filter'}),
    'take': type('Take', (_Opcall,), {'module': 'std', 'opname': 'take'}),
})


class Transform(Expr):
    def __init__(self, table, name=None):
        super(Transform, self).__init__(name=name)
        self.table = table
        self.name = name
        self.opcalls = []
        table.edge(self)

    def using(self, *opcalls):
        for opcall in opcalls:
            if not isinstance(opcall, _Opcall):
                raise InvalidQueryException(
                    "'into' method of serialize only take operator calls but received '%s' instead" % opcall)
            for k, v in opcall.kwargs.items():
                v.edge(self)

        self.opcalls = opcalls
        return self


class _At(Expr):
    pass


at = _At()
