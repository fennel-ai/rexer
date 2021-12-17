from expr import Int, Double, Bool, String, Binary, Constant
from expr import InvalidQueryException, List, Dict, Transform, Table, Var, at


class Visitor(object):
    def visit(self, obj):
        if isinstance(obj, Int):
            ret = self.visitInt(obj)

        elif isinstance(obj, Double):
            ret = self.visitDouble(obj)

        elif isinstance(obj, Bool):
            ret = self.visitBool(obj)

        elif isinstance(obj, String):
            ret = self.visitString(obj)

        elif isinstance(obj, List):
            ret = self.visitList(obj)

        elif isinstance(obj, Dict):
            ret = self.visitDict(obj)

        elif isinstance(obj, Table):
            ret = self.visitTable(obj)

        elif isinstance(obj, Transform):
            ret = self.visitTransform(obj)

        elif isinstance(obj, Var):
            ret = self.visitVar(obj)

        elif isinstance(obj, Binary):
            ret = self.visitBinary(obj)
        elif obj is at:
            ret = self.visitAt(obj)

        else:
            raise InvalidQueryException('unrecognized node type: %s', self)

        return ret

    def visitConstant(self, obj):
        raise NotImplementedError()

    def visitInt(self, obj):
        return self.visitConstant(obj)

    def visitDouble(self, obj):
        return self.visitConstant(obj)

    def visitBool(self, obj):
        return self.visitConstant(obj)

    def visitString(self, obj):
        return self.visitConstant(obj)

    def visitList(self, obj):
        raise NotImplementedError()

    def visitDict(self, obj):
        raise NotImplementedError()

    def visitTable(self, obj):
        raise NotImplementedError()

    def visitTransform(self, obj):
        raise NotImplementedError()

    def visitVar(self, obj):
        raise NotImplementedError()

    def visitAt(self, obj):
        raise NotImplementedError()

    def visitBinary(self, obj):
        raise NotImplementedError()


class Printer(Visitor):
    # TODO: if a node is a part of graph but not on path back from target, currently we won't
    # print it. Should we tackle it by printing every node reachable from every node we reach?
    def __init__(self):
        super(Printer, self).__init__()
        self.cache = {}
        self.lines = []

    def print(self, obj, varvalues=None):
        varvalues = {} if varvalues is None else varvalues

        for var, value in varvalues.items():
            if not isinstance(value, Constant):
                raise InvalidQueryException(
                    'Variable value can only be a constant but given %s for %s' % (value, var))

        for var, value in varvalues.items():
            self.lines.append('%s = %s;' %
                              (var.varname(dollar=False), self.visit(value)))

        last = self.visit(obj)
        self.lines.append(last)
        return '\n'.join(self.lines)

    def visit(self, obj):
        if obj in self.cache:
            return self.cache[obj]

        ret = super(Printer, self).visit(obj)
        self.cache[obj] = ret
        return ret

    def maybe_create_var(self, obj, rep):
        if obj.num_out_edges() > 1 or (obj.name is not None):
            self.lines.append('%s = %s;' % (obj.varname(dollar=False), rep))
            return obj.varname(dollar=True)
        else:
            return rep

    def visitConstant(self, obj):
        rep = str(obj.c)
        return self.maybe_create_var(obj, rep)

    def visitString(self, obj):
        rep = '"%s"' % obj.c
        return self.maybe_create_var(obj, rep)

    def visitBool(self, obj):
        if obj.c is True:
            rep = 'true'
        else:
            rep = 'false'
        return self.maybe_create_var(obj, rep)

    def visitList(self, obj):
        rep = '[%s]' % ', '.join(self.visit(v) for v in obj.values)
        return self.maybe_create_var(obj, rep)

    def visitList(self, obj):
        rep = '[%s]' % ', '.join(self.visit(v) for v in obj.values)
        return self.maybe_create_var(obj, rep)

    def visitDict(self, obj):
        rep = '{%s}' % ', '.join('%s=%s' % (k, self.visit(v))
                                 for k, v in obj.kwargs.items())
        return self.maybe_create_var(obj, rep)

    def visitAt(self, obj):
        return '@'

    def visitVar(self, obj):
        return obj.varname(dollar=True)

    def visitTable(self, obj):
        rep = 'table(%s)' % self.visit(obj.inner)
        return self.maybe_create_var(obj, rep)

    def visitTransform(self, obj):
        opcallstrs = []
        for opcall in obj.opcalls:
            kwargstr = ', '.join('%s=%s' % (k, self.visit(v))
                                 for k, v in opcall.kwargs.items())
            opcallstr = " | %s.%s(%s)" % (
                opcall.module, opcall.opname, kwargstr)
            opcallstrs.append(opcallstr)

        rep = '%s%s' % (self.visit(obj.table), ''.join(opcallstrs))
        return self.maybe_create_var(obj, rep)

    def visitBinary(self, obj):
        if obj.op == '.':
            # for attribute lookups, right side is just a string, not a full expression
            rep = '%s.%s' % (self.visit(obj.left), obj.right)
        elif obj.op == '[]':
            rep = '%s[%s]' % (self.visit(obj.left), self.visit(obj.right))
        else:
            rep = '%s %s %s' % (self.visit(obj.left),
                                obj.op, self.visit(obj.right))
        return self.maybe_create_var(obj, rep)


class Checker(Visitor):
    pass
