package starql.ast

import starql.lexer.Token

class LocalFinder : Visitor<Boolean> {
    override fun visitBinary(left: Ast, op: Token, right: Ast): Boolean {
        return left.accept(this) || right.accept(this)
    }

    override fun visitGrouping(inner: Ast): Boolean {
        return inner.accept(this)
    }

    override fun visitUnary(op: Token, right: Ast): Boolean {
        return right.accept(this)
    }

    override fun visitAtom(t: Token): Boolean {
        TODO("Not yet implemented")
    }

    override fun visitList(elements: ArrayList<Ast>): Boolean {
        return elements.any { it.accept(this) }
    }

    override fun visitDict(elements: HashMap<Token, Ast>): Boolean {
        return elements.values.any { it.accept(this) }
    }

    override fun visitVar(name: Token, lookups: ArrayList<Ast>): Boolean {
        TODO("Not yet implemented")
    }

    override fun visitStatement(name: Token?, body: Ast): Boolean {
        return body.accept(this)
    }

    override fun visitQuery(statements: ArrayList<Ast>): Boolean {
        return statements.any { it.accept(this) }
    }

    override fun visitTable(inner: Ast): Boolean {
        TODO("Not yet implemented")
    }

    override fun visitOpcall(operand: Ast, module: Token, name: Token, args: Map<Token, Ast>): Boolean {
        TODO("Not yet implemented")
    }

}