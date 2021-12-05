package starql.interpreter

import starql.EvalException
import starql.ast.Ast
import starql.ast.Visitor
import starql.lexer.Token
import starql.lexer.TokenType
import starql.types.Bool
import starql.types.Num
import starql.types.Str
import starql.types.Value
import java.lang.Double.parseDouble

class Interpreter : Visitor<Value> {
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
            TokenType.Number -> Num(parseDouble(t.toString()))
            TokenType.Bool -> Bool(t.toString() == "true")
            TokenType.String -> Str(t.toString())
            else -> throw EvalException("$t is not a valid atom")
        }
    }

}