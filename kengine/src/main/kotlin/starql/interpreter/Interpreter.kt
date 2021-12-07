package starql.interpreter

import starql.EvalException
import starql.ast.Ast
import starql.ast.Atom
import starql.ast.Visitor
import starql.lexer.Token
import starql.lexer.TokenType
import starql.types.*
import starql.types.Float
import starql.types.List
import java.lang.Double.parseDouble
import java.lang.Integer.parseInt
import java.util.*
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.set

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
            TokenType.Or -> l.or(r)
            TokenType.And -> l.and(r)
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
            TokenType.String -> Str(t.literal())
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

            // calculate what this ast evals to
            // note: if ast is an identifier, it doesn't know how to eval so we handle it directly
            val idx = if (ast is Atom && ast.token.type == TokenType.Identifier) {
                Str(ast.token.literal())
            } else {
                ast.accept(this)
            }
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

    override fun visitTable(inner: Ast): Value {
        val v = inner.accept(this)
        return when {
            v is List && v.l.size == 0 -> throw  EvalException("cannot initialize table from empty list")
            v is List -> {
                var d = v.l[0]
                if (d !is Dict) {
                    throw EvalException("only lists of dicts can be initialized to tables")
                }
                // v is list, has non zero size and first element is Dict
                val (schema, row) = dictToTable(d)
                val rows = arrayListOf(row)
                for (i in 1 until v.l.size) {
                    v.l[i].let {
                        if (it !is Dict) {
                            throw EvalException("all elements of list should be dicts with same keys to initialize as table")
                        }
                        val (s2, r2) = dictToTable(it)
                        if (!(schema contentEquals s2)) {
                            throw EvalException(
                                "all elements of list should be dicts with same keys to initialize as table. Old: ${
                                    Arrays.toString(schema)
                                }, new: ${Arrays.toString(s2)}"
                            )
                        }
                        rows.add(r2)
                    }
                }
                Table(schema, rows)
            }
            v is Dict -> {
                // we will now return a single row table where dict keys become schema
                val (schema, row) = dictToTable(v)
                Table(schema, arrayListOf(row))
            }
            else -> throw EvalException("can not initialize table from '$v', only lists/dicts allowed")
        }
    }

    override fun visitQuery(statements: ArrayList<Ast>): Value {
        if (statements.isEmpty()) {
            throw EvalException("query should not be empty")
        }
        return statements.map { it.accept(this) }.last()
    }
}

private fun dictToTable(d: Dict): Pair<Array<String>, Array<Value>> {
    // TODO: this does lots of copies / sorts -- make it faster if needed
    val flatmap: HashMap<String, Value> = d.flatten()
    val schema = flatmap.keys.toTypedArray()
    // sort schema so that all dicts of same type return same schema
    schema.sort()
    val rowlist = ArrayList<Value>()
    for (k in schema) {
        rowlist.add(flatmap[k]!!)
    }
    val row = rowlist.toTypedArray()
    return Pair<Array<String>, Array<Value>>(schema, row)
}