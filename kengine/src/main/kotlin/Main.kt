import starql.lexer.Lexer
import starql.parser.Parser
import starql.types.Value
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    val queries = listOf(
        "1.5 + 3 / 2 - 2;",
        "[1, \"hi\", true, false, 4.0];",
        "[1, \"hi\", true, false, 4.0, ];",
        "[];",
        "{};",
        "{hi=5, bye=\"bye\"};",
        "{hi=5, bye=\"bye\",};",
        "(1);",
        "({x=[1, 2,], y=4,});",
//        "\$x",
//        "\$x.y",
//        "\$x.y[3+5].z",
        "x = 2; \$x + 3;"
//        bigquery(),
    )
    for (query in queries) {
        println("Starting to process: $query")
        val lexer = Lexer(query)
        val rep: String
        val result: Value
        val elapsed = measureTimeMillis {
            val parser = Parser(query)
            val ast = parser.parse()
            rep = ast.toString()
            result = ast.eval()
        }
        println("[Elapsed: $elapsed ms] Query: $query, rep: $rep, result: $result")
    }
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