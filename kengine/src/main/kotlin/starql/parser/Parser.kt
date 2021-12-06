package starql.parser

import starql.ParseException
import starql.ast.*
import starql.ast.List
import starql.lexer.Lexer
import starql.lexer.Token
import starql.lexer.TokenType

/*
query          → statement +
statement      → (identifier "=")? expression ";"
expression     → logic_or;  // just an alias

logic_or       → logic_and ( "or" logic_and )*
logic_and      → equality ( "and" equality )*
equality       → comparison ( ( "!=" | "==" ) comparison )*
comparison     → term ( ( ">" | ">=" | "<" | "<=" ) term )*

term           → factor ( ( "-" | "+" ) factor )*
factor         → unary ( ( "/" | "*" ) unary )*
unary          → ( "!" | "-" ) primary | primary
primary        → "true" | "false" | Number | String | list | dict | "(" expression ")" | variable
list           → "[" expression ("," expression)* ","? "]"
dict           → "{" identifier "=" expression ("," identifier "=" expression)* ","? "]"
variable       → "$" identifier lookup*
lookup         → ("." identifier) |  "[" expression "]"

 */

class Parser(private val query: String) {
    private val lexer = Lexer(query)
    private var current: Token? = null
    private var next: Token = lexer.next()

    private fun advance(): Token {
        current = next
        next = lexer.next()
        return next
    }

    private fun matches(vararg types: TokenType): Boolean {
        return if (next.type in types) {
            advance()
            true
        } else {
            false
        }
    }

    private fun term(): Ast {
        var l = factor()
        while (matches(TokenType.Plus, TokenType.Minus)) {
            val op = current!!
            val r = factor()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun unary(): Ast {
        return if (matches(TokenType.Bang, TokenType.Minus)) {
            val op = current!!
            val v = primary()
            Unary(op, v)
        } else {
            primary()
        }
    }

    private fun factor(): Ast {
        var l = unary()
        while (matches(TokenType.Star, TokenType.Slash)) {
            val op = current!!
            val r = unary()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun primary(): Ast {
        advance()
        return when (current!!.type) {
            TokenType.Number, TokenType.Bool, TokenType.String -> Atom(current!!)
            TokenType.ListBegin -> list(true)
            TokenType.RecordBegin -> dict(true)
            TokenType.LeftParen -> grouping(true)
            TokenType.Dollar -> variable(true)
            else -> throw ParseException("expected number/bool/string but got $next")
        }
    }

    private fun expect(vararg types: TokenType): Token {
        return if (next.type in types) {
            advance()
            current!!
        } else {
            throw ParseException("unexpected token: '$next'. Expected one of $types")
        }
    }

    private fun list(prefixDone: Boolean): Ast {
        if (!prefixDone) {
            expect(TokenType.ListBegin)
        }
        val elements = arrayListOf<Ast>()
        while (next.type != TokenType.ListEnd) {
            elements.add(expression())
            when (next.type) {
                TokenType.ListEnd -> break
                TokenType.Comma -> advance()
                else -> throw ParseException("unexpected token : '$next'. expected ',' or ']'")
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
        while (next.type != TokenType.RecordEnd) {
            val (identifier, exp) = parameter()
            elements[identifier] = exp
            when (next.type) {
                TokenType.RecordEnd -> break
                TokenType.Comma -> advance()
                else -> throw ParseException("unexpected token : '$next'. expected ',' or '}'")
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
            val op = current!!
            val r = and()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun and(): Ast {
        var l = equality()
        while (matches(TokenType.And)) {
            val op = current!!
            val r = equality()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun comparison(): Ast {
        var l = term()
        while (matches(TokenType.Greater, TokenType.GreaterEqual, TokenType.Lesser, TokenType.LesserEqual)) {
            val op = current!!
            val r = term()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun grouping(prefixDone: Boolean): Ast {
        if (!prefixDone) {
            expect(TokenType.LeftParen)
        }
        val ret = expression()
        expect(TokenType.RightParen)
        return Grouping(ret)
    }

    private fun equality(): Ast {
        var l = comparison()
        while (matches(TokenType.BangEqual, TokenType.EqualEqual)) {
            val op = current!!
            val r = comparison()
            l = Binary(l, op, r)
        }
        return l
    }

    private fun expression(): Ast {
        return or()
    }

    private fun variable(prefixDone: Boolean): Ast {
        if (!prefixDone) {
            expect(TokenType.Dollar)
        }
        val v = expect(TokenType.Identifier)
        val lookups = ArrayList<Ast>()
        while (true) {
            when (next.type) {
                TokenType.Dot -> {
                    advance()
                    val id = expect(TokenType.Identifier)
                    lookups.add(Atom(id))
                }
                TokenType.ListBegin -> {
                    advance()
                    val e = expression()
                    expect(TokenType.ListEnd)
                    lookups.add(e)
                }
                else -> break
            }
        }
        return Var(v, lookups)
    }

    private fun statement(): Ast {
        var name: Token? = null
        if (next.type == TokenType.Identifier) {
            advance()
            name = current
            expect(TokenType.Equal)
        }
        val body = expression()
        expect(TokenType.Semicolon)
        return Statement(name, body)
    }

    private fun query(): Ast {
        val statements = ArrayList<Ast>(listOf(statement()))
        while (!matches(TokenType.Eof)) {
            statements.add(statement())
        }
        return Query(statements)
    }

    fun parse(): Ast {
        val r = query()
        if (!matches(TokenType.Eof)) {
            throw ParseException("unmatched tokens starting at $next")
        }
        return r
    }
}