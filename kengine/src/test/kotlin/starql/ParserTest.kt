import kotlin.test.Test
import kotlin.test.assertEquals
import starql.parser.Parser

internal class ParserTest {
    @Test
    fun testBasic() {
        val tests = arrayListOf(
            Pair("2 + 5;", "(2 + 5);\n"),
            Pair("2 + 3 * 2 - 1;", "((2 + (3 * 2)) - 1);\n")
        )
        for ((query, expected) in tests) {
            val parser = Parser(query)
            assertEquals(expected, parser.parse().toString())
        }
    }
}