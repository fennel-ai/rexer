package starql.ast

import starql.ParseException
import starql.lexer.Token
import starql.lexer.TokenType

class Printer : Visitor<String> {
    override fun visitBinary(left: Ast, op: Token, right: Ast): String {
        return "($left $op $right)"
    }

    override fun visitGrouping(inner: Ast): String {
        return "($inner)"
    }

    override fun visitUnary(op: Token, right: Ast): String {
        return "($op $right)"
    }

    override fun visitAtom(t: Token): String {
        return "$t"
    }

    override fun visitList(elements: ArrayList<Ast>): String {
        return "$elements"
    }

    override fun visitDict(elements: HashMap<Token, Ast>): String {
        return "$elements"
    }

    override fun visitVar(name: Token, lookups: ArrayList<Ast>): String {
        val prefix = when (name.type) {
            TokenType.Identifier -> "\$${name.literal()}"
            TokenType.At -> "@"
            else -> throw ParseException("unexpected token '$name'. This may be a bug in ast construction for variables")
        }
        return lookups.joinToString("", prefix = prefix) {
            when (it) {
                is Atom -> {
                    when (it.token.type) {
                        TokenType.Identifier -> ".${it.token.literal()}"
                        else -> "[${it.token.literal()}]"
                    }
                }
                else -> "[$it]"
            }
        }
    }

    override fun visitTable(inner: Ast): String {
        return "table($inner)"
    }

    override fun visitStatement(name: Token?, body: Ast): String {
        return when (name) {
            null -> "$body;"
            else -> "${name.literal()} = $body;"
        }
    }

    override fun visitQuery(statements: ArrayList<Ast>): String {
        return statements.joinToString { "$it\n" }
    }

    override fun visitOpcall(operand: Ast, module: Token, name: Token, args: Map<Token, Ast>): String {
        val argstr = args.map { (k, v) -> "${k.literal()}=$v" }.joinToString()
        return "$operand | $module.$name($argstr)"
    }
}