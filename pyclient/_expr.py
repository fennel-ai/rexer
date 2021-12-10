from __future__ import annotations

from munch import Munch
import random

# DESIGN:
# =======
# Every expression is a sublcass of base type called Expr. Every Expr object has
# a nodeid which is generated once when the object is created and does not change
# after that. Whenever any Expr object is created, it registers itself with global
# query object. Whenever we do an operation on an expr object, it spits out a new
# Expr object that represents the result of that operation (and that new object
# also registers itself with global query). This way, a series of expr objects
# are registered with global query.
#
# In addition to registration, expr objects also report graph relationship to
# query. For instance, if we start with an object e and do some operation on
# it that creates a new object b, we create an edge from e -> b in the query.
#
# Later when we try to 'execute' the query, we traverse this dependency graph. And
# if an expr object appears in exactly one more expression, we just inline it
# instead of printing a special line for the expression. Else, we print a line
# assigning this expression to a variable and use this variable wherever else
# this expression was needed.
#
# at() object is a kind of expression as well that more or less works out of the
# box because each at() object is a different object, hence gets a different nodeid
# hence eventually gets inlined at query generation stage.
#
# Each node can also have an optional user given name. If given, this name is used
# as the name of the variable to which this node is assigned to instead of a random
# hash. (Note: this currently has been removed and needs to be added back)
#
# TODOs:
# ======
# - How do people compose the query spread over multiple function calls?
# - Introduce back the notion of names
# - Somehow auto generate module/name (and ideally kwarg info) of operators
#   based on existing operators
# - Ensure that we never get to a place of printing @ outside of opcall
# - After the query executes, populate 'result' field on every node
# - What happens if node is created outside of query block but used inside?


class InvalidQueryException(Exception):
    pass

_query = None


class Query(object):

    def __init__(self):
        self.exprs = []
        self.vars = []
        self.num_vars = 0
        self.edges = {}

    def __enter__(self):
        global _query
        if _query is not None:
            raise InvalidQueryException('Cannot start a query within another query with block')
        _query = self
        return self

    def add(self, expr):
        if isinstance(expr, Var):
            self.vars.append(expr)
        else:
            self.exprs.append(expr)

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _query
        _query = None

    def edge(self, from_: Expr, to_: Expr):
        if from_ not in self.edges:
            self.edges[from_] = []
        self.edges[from_].append(to_)

    def compactify(self):
        for node, deps in self.edges.items():
            if len(deps) == 1:
                node.inline = True

    def execute(self, varvalues=None, printonly=False):
        # print all variables and exprs to make debugging easier
        # print('[DEBUG]', ', '.join('%s: %s' % (e.nodeid, str(e)) for e in self.exprs))
        # print('[DEBUG]', ', '.join(str(v) for v in self.vars))
        statements = []

        self.compactify()
        print('=' * 50)
        # first print all variables
        varvalues = {} if varvalues is None else varvalues
        for var in self.vars:
            if var not in varvalues:
                raise InvalidQueryException('Variable: %s not assigned a value' % var.nodeid)
            value = varvalues[var]
            if not isinstance(value, _Constant):
                raise InvalidQueryException('Variable value can only be a constant but given %s for %s' % (value, var))
            statements.append(self.print(var, value))

        # then print all expressions that are not inlined in some other expression
        for expr in self.exprs:
            if not expr.inline:
                if isinstance(expr, At):
                    raise InvalidQueryException('At expressions can only be included inside opcall arguments')
                statements.append(self.print(expr, expr))

        if printonly:
            return '\n'.join(statements)

    def print(self, node, value):
        return "%s = %s;" % (node.varname(dollar=False), value.print())


class Expr(object):
    def __init__(self, name=None):
        self.name = name
        self.nodeid = "%08x" % random.getrandbits(32)
        self.inline = False

        global _query
        if _query is not None:
            _query.add(self)

    def getname(self):
        return self.name

    def setname(self, name):
        self.name = name

    def getid(self):
        return self.nodeid

    def edge(self, node):
        global _query
        if _query is None:
            raise InvalidQueryException('Can not create non-constant expressions outside of `with Query()` block')
        _query.edge(self, node)

    def __getattr__(self, k: str):
        if not isinstance(k, str):
            raise InvalidQueryException("property lookup using '.' can only be done via constant strings but got '%s'" % type(k))
        ret = _Binary(self, '.', k)
        self.edge(ret)
        return ret

    def __getitem__(self, item):
        if not isinstance(item, Expr):
            raise InvalidQueryException("'[]' operation can only take expression but got '%s'" % type(item))
        ret = _Binary(self, '[]', item)
        self.edge(ret)
        return ret

    def __nonzero__(self):
        raise InvalidQueryException("can not convert: '%s' which is part of query graph to bool" % self)

    def __bool__(self):
        raise InvalidQueryException("can not convert: '%s' which is part of query graph to bool" % self)

    def _binary(self, op, other):
        ret = _Binary(self, op, other)
        self.edge(ret)
        other.edge(ret)
        return ret

    def __add__(self, other: Expr) -> Expr:
        if not isinstance(other, Expr):
            raise InvalidQueryException("'+' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('+', other)

    def __or__(self, other: Expr) -> Expr:
        if not isinstance(other, Expr):
            raise InvalidQueryException("'or' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('or', other)

    def __eq__(self, other: Expr) -> Expr:
        if not isinstance(other, Expr):
            raise InvalidQueryException("'==' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('==', other)

    def __ge__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'>=' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('>=', other)

    def __gt__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'>' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('>', other)

    def __sub__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'-' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('-', other)

    def __mul__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'*' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('*', other)

    def __truediv__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'/' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('/', other)

    def __mod__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'%' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('%', other)

    def __and__(self, other):
        if not isinstance(other, Expr):
            raise InvalidQueryException("'and' only allowed between two exprs but got: '%s' instead" % other)
        return self._binary('and', other)

    def __xor__(self, other):
        assert False, "binary operation 'xor' not supported by exprs"

    def varname(self, dollar=True):
        name = self.nodeid if self.name is None else self.name
        if dollar:
            return '$%s' % name
        else:
            return name

    def __str__(self):
        if self.inline:
            return self.print()
        else:
            return self.varname()

    def print(self):
        raise NotImplementedError()

    def __hash__(self):
        return hash(self.nodeid)


class _Constant(Expr):
    def __init__(self, c, name=None):
        super(_Constant, self).__init__(name=name)
        self.c = c

    def print(self):
        return str(self.c)

class Int(_Constant):
    def __init__(self, n, name=None):
        assert isinstance(n, int), "Int can only be initialized by int, but given '%s'" % n
        super(Int, self).__init__(n, name=name)

class Double(_Constant):
    def __init__(self, d, name=None):
        assert isinstance(d, float), "Double can only be initialized by float, but given '%s'" % d
        super(Double, self).__init__(d, name=name)

class Bool(_Constant):
    def __init__(self, b, name=None):
        assert isinstance(b, bool), "Bool can only be initialized by bool, but given '%s'" % b
        super(Bool, self).__init__(b, name=name)

    def print(self):
        if self.c is True:
            return 'true'
        else:
            return 'false'

class String(_Constant):
    def __init__(self, s, name=None):
        assert isinstance(s, str), "String can only be initialized by str, but given '%s'" % s
        super(String, self).__init__(s, name=name)

    def print(self):
        return '"%s"' % self.c

class _Binary(Expr):
    def __init__(self, left, op, right):
        super(_Binary, self).__init__()
        self.left = left
        self.op = op
        self.right = right

    def print(self, left_inline=False, right_inline=False):
        if self.op == '.':
            # for attribute lookups, right side is just a string, not a full expression
            return '%s.%s' % (self.left, self.right)
        elif self.op == '[]':
            return '%s[%s]' % (self.left, self.right)
        else:
            return '%s %s %s' % (self.left, self.op, self.right)


class List(Expr):
    def __init__(self, *values, name=None):
        super(List, self).__init__(name=name)
        for v in values:
            if not isinstance(v, Expr):
                raise InvalidQueryException('List can only contain StarQL expresions but got %s instead' % v)
            v.edge(self)
        self.values = values

    def print(self):
        return '[%s]' % ', '.join(str(v) for v in self.values)


class Dict(Expr):
    def __init__(self, name=None, **values):
        super(Dict, self).__init__(name=name)
        self.kwargs = {}
        for k, v in values.items():
            if not isinstance(v, Expr):
                raise InvalidQueryException('Dict values can only be StarQL expressions but got %s instead' % v)
            self.kwargs[k] = v
            v.edge(self)

    def print(self):
        return '{%s}' % ', '.join('%s=%s' % (k, str(v)) for k, v in self.kwargs.items())


class Table(Expr):
    def __init__(self, inner, name=None):
        super(Table, self).__init__(name=name)
        if not isinstance(inner, Expr):
            raise InvalidQueryException('Table can only be created from StarQL expressions but got %s instead' % inner)

        self.inner = inner
        inner.edge(self)

    def print(self):
        return 'Table(%s)' % self.inner


class Var(Expr):
    def __init__(self, name=None):
        super(Var, self).__init__(name=name)

    def print(self):
        return self.varname()

class _Opcall(object):
    # TODO: how will error handling happen if user called Opcall without table?
    module = None
    opname = None
    def __init__(self, name=None, **kwargs):
        if (self.module is None) or (self.opname is None):
            raise InvalidQueryException("operator '%s.%s' is not registered" % (self.module, self.opname))

        self.name = name
        self.kwargs = {}
        for k, v in kwargs.items():
            if not isinstance(v, Expr):
                raise InvalidQueryException("Value for operator parameter '%s' given '%s' but expected a StarQL expresion" % (k, v))
            self.kwargs[k] = v

    def __str__(self):
        kwargstr = ', '.join('%s=%s' % (k, str(v)) for k, v in self.kwargs.items())
        return "%s.%s(%s)" % (self.module, self.opname, kwargstr)


Ops = Munch()
Ops.std = Munch({
    'filter': type('Filter', (_Opcall, ), {'module': 'std', 'opname': 'filter'}),
    'take': type('Take', (_Opcall, ), {'module': 'std', 'opname': 'take'}),
})


class Transform(Expr):
    # TODO: how will error handling happen if user forgot to call using
    def __init__(self, table, name=None):
        super(Transform, self).__init__(name=name)
        self.table = table
        self.name = name
        self.opcalls = []
        table.edge(self)

    def using(self, *opcalls):
        for opcall in opcalls:
            if not isinstance(opcall, _Opcall):
                raise InvalidQueryException("'into' method of transform only takes opcalls but received '%s' instead" % opcall)
            for k, v in opcall.kwargs.items():
                v.edge(self)

        self.opcalls = opcalls
        return self

    def print(self):
        opcallstr = ''.join('| %s' % opcall for opcall in self.opcalls)
        return '%s %s' % (self.table, opcallstr)

class At(Expr):
    def print(self):
        return '@'

at = At()

if __name__ == '__main__':
    with Query() as query:
        a = Int(1, name='a')
        b = Int(2)
        b.setname('b')
        c = a + b
        c.setname('c')
        x = Var(name='x')
        l = List(a, a, Int(5), x, name='l')
        t = Table(l)
        ok = t.x.y[a]
        res = Transform(t, name='res').using(
            Ops.std.filter(where=Bool(True)),
            Ops.std.filter(where=At().x > At().y + Int(1)),
            Ops.std.take(count=Int(1)),
            Ops.std.take(where=At().z == a),
        )

    print(query.execute({x: Int(9)}, printonly=True))