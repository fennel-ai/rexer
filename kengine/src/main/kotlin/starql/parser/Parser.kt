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
expression     → logic_or ("|" operator)*
operator       → identifier "." identifier "("parameter ("," parameter)* ","? ")"

logic_or       → logic_and ( "or" logic_and )*
logic_and      → equality ( "and" equality )*
equality       → comparison ( ( "!=" | "==" ) comparison )*
comparison     → term ( ( ">" | ">=" | "<" | "<=" ) term )*

term           → factor ( ( "-" | "+" ) factor )*
factor         → unary ( ( "/" | "*" ) unary )*
unary          → ( "!" | "-" ) primary | primary
primary        → "true" | "false" | Number | String | list | dict | "(" expression ")" | variable | table
list           → "[" expression ("," expression)* ","? "]"
dict           → "{" identifier "=" expression ("," identifier "=" expression)* ","? "]"
variable       → ("$" identifier | "@") lookup*
lookup         → ("." identifier) |  "[" expression "]"

 */

class Parser(private val query: String) {
    private val lexer = Lexer(query)
    private var current: Token? = null
    private var next: Token = lexer.next()
    private val varsSoFar = HashSet<String>()
    private val errors = arrayListOf<ParseException>()
    private var needsRecovery = false

    private fun advance(): Token {
        current = next
        next = lexer.next()
        return next
    }

    private fun error(error: ParseException) {
        errors.add(error)
        needsRecovery = true
    }

    private fun recover() {
        // consumes some tokens until the next statement to fix the internal state of parser
        while (current!!.type !in listOf(TokenType.Eof, TokenType.Semicolon)) {
            advance()
        }
    }

    private fun matches(vararg types: TokenType): Boolean {
        return if (next.type in types) {
            advance()
            true
        } else {
            false
        }
    }

    private fun expect(vararg types: TokenType): Token {
        return if (next.type in types) {
            advance()
            current!!
        } else {
            throw ParseException("unexpected token '$next'", current, types.map { it.toString() })
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
            TokenType.At -> variable(true)
            TokenType.Table -> table(true)
            else -> throw ParseException(
                "unexpected token '$current'", current,
                listOf("digit", "[", "{", "(", "$", "@", "table")
            )
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
                else -> throw ParseException("unexpected token '$next'", current, listOf(",", "]"))
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
                else -> throw ParseException("unexpected token '$next'", current, listOf(",", "}"))
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
        var e = or()
        while (matches(TokenType.Pipe)) {
            val module = expect(TokenType.Identifier)
            expect(TokenType.Dot)
            val name = expect(TokenType.Identifier)
            expect(TokenType.LeftParen)
            val args = hashMapOf<Token, Ast>()
            while (next.type != TokenType.RightParen) {
                val (key, exp) = parameter()
                args[key] = exp
                when (next.type) {
                    TokenType.RightParen -> break
                    TokenType.Comma -> advance()
                    else -> throw ParseException("unexpected token '$next'", current, listOf(",", ")"))
                }
            }
            expect(TokenType.RightParen)
            e = Opcall(e, module, name, args)
        }
        return e
    }

    private fun variable(prefixDone: Boolean): Ast {
        if (!prefixDone) {
            expect(TokenType.At, TokenType.Dollar)
        }
        val v = when (current!!.type) {
            TokenType.Dollar -> {
                val name = expect(TokenType.Identifier)
                if (name.literal() !in varsSoFar) {
                    throw ParseException(
                        "referring to undefined variable: '${name.literal()}'",
                        current,
                        listOf<String>()
                    )
                }
                name
            }
            TokenType.At -> current!!
            else -> throw ParseException("unexpected token '$current'", current, listOf("@", "$"))
        }

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
            varsSoFar.add(name!!.literal())
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

    private fun table(prefixDone: Boolean): Ast {
        if (!prefixDone) {
            expect(TokenType.Table)
        }
        expect(TokenType.LeftParen)
        val e = expression()
        expect(TokenType.RightParen)
        return Table(e)
    }

    fun parse(): Ast {
        val statements = ArrayList<Ast>()
        while (!matches(TokenType.Eof)) {
            try {
                if (needsRecovery) {
                    recover()
                    needsRecovery = false
                }
                statements.add(statement())
            } catch (error: ParseException) {
                error(error)
            }
        }
        if (errors.isEmpty() && statements.isEmpty()) {
            error(ParseException("zero valid statements in query", current, listOf()))
        }
        if (errors.isNotEmpty()) {
            for (e in errors) {
                println(e)
            }
            throw errors.first()
        }
        return Query(statements)
    }
}