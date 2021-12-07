import starql.parser.Parser
import kotlin.system.measureTimeMillis

/*
TODOs
    Parser
        * Add eager ops
        * Add @
        * Improve error throwing mechanism to give positions/tokens etc.
        * add tests about errors
    Tables
        * No column should repeat in schema
        * Types of each column should be same
        * If we give a 1d or 2d list but with no dicts, we should create table with int indices as colnames
        * Add lots of tests (parser + native)
        * Use arrow format for tables
 */
fun main(args: Array<String>) {
    val toPrintOnly = listOf<String>(
        "table({x=1, y=2});",
    )
    val toEval = listOf<String>(
        "table({x=1, y=2});",
        """
            l = [{x=1, y=2.0, s="hi", b=true}, {x=3, b=false, y=4, s="bye"}]; 
            table(${'$'}l);
        """,
        """
            l = [{x=1, y=2.0, r={a=1, b=2}}, {x=3, y=4, r={b=3, a=4}}]; 
            table(${'$'}l);
        """,
    )
    for (q in toPrintOnly) {
        val ast = Parser(q).parse()
        println("[$q]: $ast")
    }
    for (q in toEval) {
        val ast = Parser(q).parse()
        println("[$q]: ${ast.eval()}")
    }
    benchmark()
}

fun benchmark() {
    val n = 100
    var total: Long = 0
    for (i in 1..n) {
        val query = bigquery(i)
        val elapsed = measureTimeMillis {
            val parser = Parser(query)
            val ast = parser.parse()
            ast.eval()
        }
        total += elapsed
    }
    println("Avg time to lex + parse: ${total / n} ms ")
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