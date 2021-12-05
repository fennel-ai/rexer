package starql.lexer

import starql.LexException

class Token(val query: String, val start: Int, val end: Int, val type: TokenType, val line: Int) {
    override fun toString(): String {
        return query.substring(start, end)
    }
}

class Lexer(private val query: String) {
    private var current = 0
    private var start = 0
    private var line = 0

    private fun generate(type: TokenType): Token {
        val token = Token(start = start, end = current, type = type, line = line, query = query)
        start = current
        return token
    }

    private fun done(): Boolean {
        return current >= query.length
    }

    private fun peek(): Char? {
        return if (!done()) query[current] else null
    }

    private fun advance(): Char? {
        return if (done()) {
            null
        } else {
            val c = query[current]
            current += 1
            c
        }
    }

    private fun identifier() {
        while (true) {
            val c = peek()
            if ((c?.isLetterOrDigit() == true) || c!! == '_') {
                advance()!!
            } else {
                break
            }
        }
    }

    private fun string() {
        while (!done()) {
            if (advance()!! == '"') {
                return
            }
        }
        throw LexException("missing closing \" for string")
    }

    private fun digits() {
        while (peek()?.isDigit() == true) {
            advance()
        }
    }

    private fun variable() {
        val c = peek()
        if (c?.equals('_') == true || c?.isLetterOrDigit() == true) {
            identifier()
        } else {
            throw LexException("'$' not followed by valid identifier name")
        }
    }

    private fun number() {
        digits()
        if (peek()?.equals('.') == true) {
            advance()!!
            if (peek()?.isDigit() == true) {
                digits()
            } else {
                throw LexException("expected digit after '.'")
            }
        }
    }

    tailrec fun next(): Token {
        return when (val c = advance()) {
            null -> generate(TokenType.Eof)
            '(' -> generate(TokenType.LeftParen)
            ')' -> generate(TokenType.RightParen)
            '[' -> generate(TokenType.ListBegin)
            ']' -> generate(TokenType.ListEnd)
            '{' -> generate(TokenType.RecordBegin)
            '}' -> generate(TokenType.RecordEnd)
            ',' -> generate(TokenType.Comma)
            '.' -> generate(TokenType.Dot)
            '|' -> generate(TokenType.Pipe)
            '+' -> generate(TokenType.Plus)
            '-' -> generate(TokenType.Minus)
            '*' -> generate(TokenType.Star)
            ';' -> generate(TokenType.Semicolon)
            '/' -> {
                if (peek()?.equals('/') == true) {
                    while (true) {
                        advance()!!
                        if (peek()?.equals('\n') == true) {
                            break
                        }
                    }
                    next()
                } else {
                    generate(TokenType.Slash)
                }
            }
            '=' -> {
                if (peek()?.equals('=') == true) {
                    advance()!!
                    generate(TokenType.EqualEqual)
                } else {
                    generate(TokenType.Equal)
                }
            }
            '>' -> {
                if (peek()?.equals('=') == true) {
                    advance()!!
                    generate(TokenType.GreaterEqual)
                } else {
                    generate(TokenType.Greater)
                }
            }
            '<' -> {
                if (peek()?.equals('=') == true) {
                    advance()!!
                    generate(TokenType.LesserEqual)
                } else {
                    generate(TokenType.Lesser)
                }
            }
            '!' -> {
                if (peek()?.equals('=') == true) {
                    advance()!!
                    generate(TokenType.BangEqual)
                } else {
                    generate(TokenType.Bang)
                }
            }
            '"' -> {
                string()
                generate(TokenType.String)
            }
            in '0'..'9' -> {
                number()
                generate(TokenType.Number)
            }
            '$' -> {
                variable()
                generate(TokenType.Variable)
            }
            in listOf(' ', '\t', '\r') -> next()
            '\n' -> {
                line += 1
                next()
            }
            in ('a'..'z').union('A'..'Z').union(listOf('_')) -> {
                identifier()
                // TODO: Can we avoid copies here?
                when (query.substring(start, current)) {
                    in listOf("true", "false") -> generate(TokenType.Bool)
                    "or" -> generate(TokenType.Or)
                    "and" -> generate(TokenType.And)
                    else -> generate(TokenType.Identifier)
                }
            }
            else -> {
                throw Exception("unexpected character: $this.query[current]")
            }
        }
    }
}

enum class TokenType {
    // Characters
    LeftParen,
    RightParen,
    ListBegin,
    ListEnd,
    RecordBegin,
    RecordEnd,
    Comma,
    Dot,
    Pipe,
    Semicolon,
    Equal,

    // Arithmetic operaotrs
    // TODO: Do we need modulo operator?
    Plus,
    Minus,
    Star,
    Slash,

    // Unary bool op.
    Bang,

    // Relational operations
    Greater,
    Lesser,
    GreaterEqual,
    LesserEqual,
    EqualEqual,
    BangEqual,

    // Keywords
    Or,
    And,

    // All rest
    Identifier,
    Variable,
    String,
    Number,
    Bool,
    Eof,
}