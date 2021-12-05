package starql.ast

import starql.interpreter.Interpreter
import starql.lexer.Token
import starql.types.Value

interface Visitor<T> {
    fun visitBinary(left: Ast, op: Token, right: Ast): T
    fun visitGrouping(inner: Ast): T
    fun visitUnary(op: Token, right: Ast): T
    fun visitAtom(t: Token): T
    //    fun visitOpexp(&self, root: &Ast, opcalls: &[OpCall]) : T;
    //    fun visitStatement(&self, variable: &Option<Token>, body: &Ast): T;
    //    fun visitQuery(&self, statements: &[Ast]) : T;
    //    fun visitList(list: Arr) : T;
    //    fun visitRecord(&self, names: &[Token], values: &[Ast]) : T;
}

sealed class Ast {
    fun <T> accept(v: Visitor<T>): T {
        return when (this) {
            is Binary -> v.visitBinary(left, op, right)
            is Atom -> v.visitAtom(token)
            is Grouping -> v.visitGrouping(inner)
            is Unary -> v.visitUnary(op, right)
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

class Printer : Visitor<String> {
    override fun visitBinary(left: Ast, op: Token, right: Ast): String {
        return "(${left.accept(this)} $op ${right.accept(this)})"
    }

    override fun visitGrouping(inner: Ast): String {
        return "(${inner.accept(this)})"
    }

    override fun visitUnary(op: Token, right: Ast): String {
        return "($op ${right.accept(this)})"
    }

    override fun visitAtom(t: Token): String {
        return "$t"
    }
}