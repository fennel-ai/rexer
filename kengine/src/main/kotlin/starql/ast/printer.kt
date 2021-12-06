package starql.ast

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
        return lookups.joinToString("", prefix = "$$name") {
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

    override fun visitStatement(name: Token?, body: Ast): String {
        return when (name) {
            null -> "$body;"
            else -> "${name.literal()} = $body;"
        }
    }

    override fun visitQuery(statements: ArrayList<Ast>): String {
        return statements.joinToString { "$it\n" }
    }
}