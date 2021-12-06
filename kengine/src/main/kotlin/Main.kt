import starql.parser.Parser
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    for (i in 1..100) {
        val query = bigquery(i)
        val elapsed = measureTimeMillis {
            val parser = Parser(query)
            val ast = parser.parse()
            ast.eval()
        }
        println("[$i]: Time to lex + parse: $elapsed ms")
    }
}

fun bigquery(offset: Int): String {
    var q = StringBuilder()
    q.append("[")
    for (i in 0..10_000) {
        q.append("{x=${i + offset}, y=-$i}, ")
    }
    q.append("];")
    return q.toString()
}