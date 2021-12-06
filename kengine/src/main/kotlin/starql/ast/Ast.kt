package starql.ast

import starql.interpreter.Interpreter
import starql.lexer.Token
import starql.types.Value

interface Visitor<T> {
    fun visitBinary(left: Ast, op: Token, right: Ast): T
    fun visitGrouping(inner: Ast): T
    fun visitUnary(op: Token, right: Ast): T
    fun visitAtom(t: Token): T
    fun visitList(elements: ArrayList<Ast>): T
    fun visitDict(elements: HashMap<Token, Ast>): T
    fun visitVar(name: Token, lookups: ArrayList<Ast>): T
    fun visitStatement(name: Token?, body: Ast): T
    fun visitQuery(statements: ArrayList<Ast>): T
    //    fun visitOpexp(&self, root: &Ast, opcalls: &[OpCall]) : T;
}

sealed class Ast {
    fun <T> accept(v: Visitor<T>): T {
        return when (this) {
            is Binary -> v.visitBinary(left, op, right)
            is Atom -> v.visitAtom(token)
            is Grouping -> v.visitGrouping(inner)
            is Unary -> v.visitUnary(op, right)
            is List -> v.visitList(elements)
            is Dict -> v.visitDict(elements)
            is Var -> v.visitVar(name, lookups)
            is Statement -> v.visitStatement(name, body)
            is Query -> v.visitQuery(statements)
        }
    }

    override fun toString(): String {
        val printer = Printer()
        return this.accept(printer)
    }

    fun eval(): Value {
        val interpreter = Interpreter()
        return this.accept(interpreter)
    }
}

class Binary(val left: Ast, val op: Token, val right: Ast) : Ast()
class Atom(val token: Token) : Ast()
class Grouping(val inner: Ast) : Ast()
class Unary(val op: Token, val right: Ast) : Ast()
class List(val elements: ArrayList<Ast>) : Ast()
class Dict(val elements: HashMap<Token, Ast>) : Ast()
class Statement(val name: Token?, val body: Ast) : Ast()
class Query(val statements: ArrayList<Ast>) : Ast()

// "." identifier gets an atom with identifier token and [exp] gets full ast for exp
class Var(val name: Token, val lookups: ArrayList<Ast>) : Ast()

