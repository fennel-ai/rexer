package starql.interpreter

import starql.EvalException
import starql.ast.Ast
import starql.ast.Visitor
import starql.lexer.Token
import starql.lexer.TokenType
import starql.types.*
import starql.types.Float
import starql.types.List
import java.lang.Double.parseDouble
import java.lang.Integer.parseInt

class Interpreter : Visitor<Value> {
    private val env = Environment(null)


    override fun visitBinary(left: Ast, op: Token, right: Ast): Value {
        val l = left.accept(this)
        val r = right.accept(this)
        return when (op.type) {
            TokenType.Minus -> l - r
            TokenType.Plus -> l + r
            TokenType.Star -> l * r
            TokenType.Slash -> l / r
            TokenType.Greater -> Bool(l > r)
            TokenType.GreaterEqual -> Bool(l >= r)
            TokenType.Lesser -> Bool(l < r)
            TokenType.LesserEqual -> Bool(l <= r)
            TokenType.EqualEqual -> Bool(l == r)
            TokenType.BangEqual -> Bool(l != r)
            else -> throw EvalException("unsupported binary operation '$op'")
        }
    }

    override fun visitGrouping(inner: Ast): Value {
        return inner.accept(this)
    }

    override fun visitUnary(op: Token, right: Ast): Value {
        val r = right.accept(this)
        return when (op.type) {
            TokenType.Minus -> -r
            TokenType.Bang -> !r
            else -> throw EvalException("$op is not a unary operation")
        }
    }

    override fun visitAtom(t: Token): Value {
        return when (t.type) {
            TokenType.Number -> {
                if ('.' in t.literal()) {
                    Float(parseDouble(t.toString()))
                } else {
                    Int64(parseInt(t.toString()))
                }
            }
            TokenType.Bool -> Bool(t.toString() == "true")
            TokenType.String -> Str(t.toString())
            else -> throw EvalException("$t is not a valid atom")
        }
    }

    override fun visitList(elements: ArrayList<Ast>): Value {
        return List(ArrayList(elements.map { it.accept(this) }))
    }

    override fun visitDict(elements: HashMap<Token, Ast>): Value {
        val m = HashMap<String, Value>()
        for ((t, ast) in elements) {
            m[t.literal()] = ast.accept(this)
        }
        return Dict(m)
    }

    override fun visitVar(name: Token, lookups: ArrayList<Ast>): Value {
        var base: Value? = env.get(name) ?: throw EvalException("cannot access undefined variable: '$name'")
        for (ast in lookups) {
            val prev = base
            val idx = ast.accept(this)
            base = when {
                base is List && idx is Int64 -> base.l.getOrNull(idx.n)
                base is Dict && idx is Str -> base.m[idx.s]
                else -> throw EvalException("property lookup only supported on lists/dicts")
            }
            if (base == null) {
                throw EvalException("accessing undefined property $idx on $prev")
            }
        }
        return base!!
    }

    override fun visitStatement(name: Token?, body: Ast): Value {
        val res = body.accept(this)
        if (name != null) {
            env.define(name, res)
        }
        return res
    }

    override fun visitQuery(statements: ArrayList<Ast>): Value {
        if (statements.isEmpty()) {
            throw EvalException("query should not be empty")
        }
        return statements.map { it.accept(this) }.last()
    }
}