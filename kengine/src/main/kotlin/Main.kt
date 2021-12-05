import starql.lexer.Lexer
import starql.lexer.TokenType
import starql.parser.Parser
import starql.types.Value
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    val queries = listOf(
        "x = 05.13;  z = \"foo\"; y = [3, x, 4] | incr(by=x,do=5>=7)",
        bigquery()
    )
    for (query in queries) {
        val lexer = Lexer(query)
        val elapsed = measureTimeMillis {
            while (true) {
                val token = lexer.next()
                if (token.type == TokenType.Eof) {
                    break
                }
            }
        }
        println("Lexed query in $elapsed ms")
    }
    var result: Value? = null
    val elapsed = measureTimeMillis {
        val q = "1.5 + 3 / 2 - 2"
        for (i in 0..0) {
            val parser = Parser(q)
            val ast = parser.parse()
            println(ast)
            result = ast.eval()
        }
    }
    println("Eval query in $elapsed ms: $result")
}

fun bigquery(): String {
    var q = StringBuilder()
    q.append("[")
    for (i in 0..10_000) {
        q.append("{x=$i, y=$i}, ")
    }
    q.append(']')
    return q.toString()
}