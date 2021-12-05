package starql.parser

import starql.ParseException
import starql.ast.*
import starql.ast.List
import starql.lexer.Lexer
import starql.lexer.Token
import starql.lexer.TokenType

/*
expression     → logic_or;  // just an alias

logic_or       → logic_and ( "or" logic_and )* ;
logic_and      → equality ( "and" equality )* ;
equality       → comparison ( ( "!=" | "==" ) comparison )* ;
comparison     → term ( ( ">" | ">=" | "<" | "<=" ) term )* ;

term           → factor ( ( "-" | "+" ) factor )* ;
factor         → unary ( ( "/" | "*" ) unary )* ;
unary          → ( "!" | "-" ) primary | primary ;
primary        → "true" | "false" | Number | String | list | dict;
list           → "[" expression ("," expression)* ","? "]"
dict           → "{" identifier "=" expression ("," identifier "=" expression)* ","? "]"

 */

class Parser(private val query: String) {
    private val lexer = Lexer(query)
    private var previous: Token? = null
    private var current: Token = lexer.next()

    private fun advance(): Token {
        previous = current
        current = lexer.next()
        return current
    }

    private fun matches(vararg types: TokenType): Boolean {
        return if (current.type in types) {
            advance()
            true
        } else {
            false
        }
    }

    private fun term(): Ast {
        var l = factor()
        while (matches(TokenType.Plus, TokenType.Minus)) {
            val op = previous!!
            val r = factor()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun unary(): Ast {
        return if (matches(TokenType.Bang, TokenType.Minus)) {
            val op = previous!!
            val v = primary()
            Unary(op, v)
        } else {
            primary()
        }
    }

    private fun factor(): Ast {
        var l = unary()
        while (matches(TokenType.Star, TokenType.Slash)) {
            val op = previous!!
            val r = unary()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun primary(): Ast {
        advance()
        return when (previous!!.type) {
            TokenType.Number, TokenType.Bool, TokenType.String -> Atom(previous!!)
            TokenType.ListBegin -> list(true)
            TokenType.RecordBegin -> dict(true)
            else -> throw ParseException("expected number/bool/string but got $current")
        }
    }

    private fun expect(vararg types: TokenType): Token {
        return if (current.type in types) {
            advance()
            previous!!
        } else {
            throw ParseException("unexpected token: '$current'. Expected one of $types")
        }
    }

    private fun list(prefixDone: Boolean): Ast {
        if (!prefixDone) {
            expect(TokenType.ListBegin)
        }
        val elements = arrayListOf<Ast>()
        while (current.type != TokenType.ListEnd) {
            elements.add(expression())
            when (current.type) {
                TokenType.ListEnd -> break
                TokenType.Comma -> advance()
                else -> throw ParseException("unexpected token : '$current'. expected ',' or ']'")
            }
        }
        expect(TokenType.ListEnd)
        return List(elements)
    }

    private fun dict(prefixDone: Boolean): Ast {
        if (!prefixDone) {
            expect(TokenType.RecordBegin)
        }
        val elements = HashMap<Token, Ast>()
        while (current.type != TokenType.RecordEnd) {
            val (identifier, exp) = parameter()
            elements[identifier] = exp
            when (current.type) {
                TokenType.RecordEnd -> break
                TokenType.Comma -> advance()
                else -> throw ParseException("unexpected token : '$current'. expected ',' or '}'")
            }
        }
        expect(TokenType.RecordEnd)
        return Dict(elements)
    }

    private fun parameter(): Pair<Token, Ast> {
        val p = expect(TokenType.Identifier)
        expect(TokenType.Equal)
        val e = expression()
        return Pair<Token, Ast>(p, e)
    }

    private fun or(): Ast {
        var l = and()
        while (matches(TokenType.Or)) {
            val op = previous!!
            val r = and()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun and(): Ast {
        var l = equality()
        while (matches(TokenType.And)) {
            val op = previous!!
            val r = equality()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun comparison(): Ast {
        var l = term()
        while (matches(TokenType.Greater, TokenType.GreaterEqual, TokenType.Lesser, TokenType.LesserEqual)) {
            val op = previous!!
            val r = term()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun equality(): Ast {
        var l = comparison()
        while (matches(TokenType.BangEqual, TokenType.EqualEqual)) {
            val op = previous!!
            val r = comparison()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun expression(): Ast {
        return or()
    }

    fun parse(): Ast {
        val r = expression()
        if (!matches(TokenType.Eof)) {
            throw ParseException("unmatched tokens starting at $current")
        }
        return r
    }

}