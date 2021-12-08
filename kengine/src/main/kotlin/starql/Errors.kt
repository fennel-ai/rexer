package starql

import starql.lexer.Token

class ParseException(private val msg: String, private val token: Token?, private val altChoices: List<String>) :

    Exception(msg) {
    override fun toString(): String {
        val sb = StringBuilder()
        if (token != null) {
            sb.append("[Line:${token.line}:${token.col}] ")
        }
        sb.append("Error: $msg")
        if (altChoices.isNotEmpty()) {
            sb.append(" when expected ${altChoices.joinToString(separator = " or ") { it -> "'$it'" }}")
        }
        return sb.toString()
    }
}

class EvalException(private val msg: String) : RuntimeException(msg)