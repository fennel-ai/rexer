from gen import ast_pb2 as proto
from rql import visitor


class Serializer(visitor.Visitor):
    def __init__(self):
        super(Serializer, self).__init__()
        self.cache = {}
        self.statements = []

    def visit(self, obj):
        if obj in self.cache:
            return self.cache[obj]

        ret = super(Serializer, self).visit(obj)
        self.cache[obj] = ret
        return ret

    def serialize(self, obj):
        last = self.visit(obj)
        ret = proto.Ast()
        ret.query.SetInParent()
        for s in self.statements:
            ret.query.statements.append(s)

        last_s = proto.Statement()
        last_s.body.CopyFrom(last)
        ret.query.statements.append(last_s)
        return ret

    def maybe_create_var(self, obj, ast):
        if obj.num_out_edges() > 1 or (obj.name is not None):
            statement = proto.Statement()
            statement.name = obj.varname(dollar=False)
            statement.body.CopyFrom(ast)
            self.statements.append(statement)
            var = proto.Ast()
            var.var.name = obj.varname(dollar=False)
            return var
        else:
            return ast

    def visitConstant(self, obj):
        raise Exception('this should never happen')

    def visitInt(self, obj):
        ast = proto.Ast()
        ast.atom.int = obj.c
        return self.maybe_create_var(obj, ast)

    def visitDouble(self, obj):
        ast = proto.Ast()
        ast.atom.double = obj.c
        return self.maybe_create_var(obj, ast)

    def visitBool(self, obj):
        ast = proto.Ast()
        ast.atom.bool = obj.c
        return self.maybe_create_var(obj, ast)

    def visitString(self, obj):
        ast = proto.Ast()
        ast.atom.string = obj.c
        return self.maybe_create_var(obj, ast)

    def visitList(self, obj):
        ast = proto.Ast()
        ast.list.SetInParent()
        for v in obj.values:
            pv = self.visit(v)
            ast.list.values.append(pv)
        return self.maybe_create_var(obj, ast)

    def visitDict(self, obj):
        ast = proto.Ast()
        ast.dict.SetInParent()
        for k, v in obj.kwargs.items():
            pv = self.visit(v)
            ast.dict.values[k].CopyFrom(pv)
        return self.maybe_create_var(obj, ast)

    def visitVar(self, obj):
        ast = proto.Ast()
        ast.var.name = obj.name
        return self.maybe_create_var(obj, ast)

    def visitAt(self, obj):
        ast = proto.Ast()
        ast.at.SetInParent()
        # we don't want to create a var for at in any situation
        return ast

    def visitBinary(self, obj):
        ast = proto.Ast()
        ast.binary.left.CopyFrom(self.visit(obj.left))
        ast.binary.right.CopyFrom(self.visit(obj.right))
        ast.binary.op = obj.op
        return self.maybe_create_var(obj, ast)

    def visitTable(self, obj):
        ast = proto.Ast()
        ast.table.inner.CopyFrom(self.visit(obj.inner))
        return self.maybe_create_var(obj, ast)

    def visitOpcall(self, obj):
        ast = proto.Ast()
        ast.opcall.SetInParent()
        ast.opcall.operand.CopyFrom(self.visit(obj.operand))
        ast.opcall.namespace = obj.operator.module
        ast.opcall.name = obj.operator.opname
        kwargs = proto.Dict()
        for k, v in obj.operator.kwargs.items():
            kwargs.values[k].CopyFrom(self.visit(v))
        ast.opcall.kwargs.CopyFrom(kwargs)
        return self.maybe_create_var(obj, ast)

    def visitLookup(self, obj):
        ast = proto.Ast()
        ast.lookup.on.CopyFrom(self.visit(obj.on))
        ast.lookup.property = obj.property
        return self.maybe_create_var(obj, ast)
    
    def visitIfelse(self, obj):
        ast = proto.Ast()
        ast.ifelse.condition.CopyFrom(self.visit(obj.condition))
        ast.ifelse.then_do.CopyFrom(self.visit(obj.then_do))
        ast.ifelse.else_do.CopyFrom(self.visit(obj.then_do))
        return self.maybe_create_var(obj, ast)
