import starql.parser.Parser
import kotlin.system.measureTimeMillis

/*
TODOs
    Lexer
        * Add checks for delimiters and no unnecessary white spaces
    Parser
        * Improve error throwing mechanism to give positions/tokens etc.
        * add tests about errors
    Tables
        * No column should repeat in schema
        * Types of each column should be same
        * If we give a 1d or 2d list but with no dicts, we should create table with int indices as colnames
        * Add lots of tests (parser + native)
        * Use arrow format for tables
    Operator
        * Some transparent way of registration
        * Ideally, no type errors for their parameters - if any, exceptions are handled by us outside apply
        * Params should be readonly so that no one can destroy it
        * People should not be able to give any arbitrary module name - ideally it is specified only once
        * Figure out what to do with tables - do we need both tables/lists?
        * What should functions like last/first return? List or value?
        * Think about immutability of input/output streams - who can destroy what and how do we enforce it (+ tests)

    Types
        * Do we need Value to be a special class? What if it was just an interface implemented by "normal" types?
 */
fun main(args: Array<String>) {
    val toPrintOnly = listOf<String>(
        "table({x=1, y=2});",
        "5 | std.op(hi=@.x, bye=3,);",

        "5 | std.op(hi=1, bye=3,) | new.new(yo=false, hi=\"yo\");"
    )
    val toEval = listOf<String>(
//        """
//            [1, 2, 3, 4] | std.filter(where=@ > 1) | std.take(limit=2) | std.last();
//        """,
        """ 
            x = [{a= 1, b=1}, {a=2, b=2}, {a=3 b=3}, {a=4, b=4}];
            ${'$'}x | std.filter(where=@.a * @. > 3) | std.take(limit=2) | std.last();
        """
//        "5 | std.op(hi=1, bye=3,) | new.new(yo=false, hi=\"yo\");"
//        "table({x=1, y=2});",
//        """
//            l = [{x=1, y=2.0, s="hi", b=true}, {x=3, b=false, y=4, s="bye"}];
//            table(${'$'}l) | std.op(hi=1, bye=3);
//        """,
//        """
//            l = [{x=1, y=2.0, r={a=1, b=2}}, {x=3, y=4, r={b=3, a=4}}];
//            table(${'$'}l);
//        """,
    )
    for (q in toPrintOnly) {
        val ast = Parser(q).parse()
        println("[$q]: $ast")
    }
    for (q in toEval) {
        val ast = Parser(q).parse()
        println("Query: $q:: ${ast.eval()}")
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