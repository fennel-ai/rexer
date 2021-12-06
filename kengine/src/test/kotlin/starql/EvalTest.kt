import kotlin.test.Test
import kotlin.test.assertEquals
import starql.parser.Parser
import starql.types.*
import starql.types.List

internal class EvalTest {
    @Test
    fun testBasic() {
        val tests = mapOf(
            // arithmetic
            "2 + 5;" to Int64(7),
            "2 + 3 * 2 - 1;" to Int64(7),
            "(2 + 3) * 2 - 1;" to Int64(9),
            "(2 + 3) / 5 ;" to Float(1.0),
            "(2 + 3) / 10 ;" to Float(0.5),
            "(2 + 3) / 5 + 5 ;" to Float(6.0),
            "-2 + 5;" to Int64(3),
            "-(2 + 5);" to Int64(-7),

            // bool literal and operations
            "true;" to Bool(true),
            "false;" to Bool(false),
            "true or false;" to Bool(true),
            "true or true;" to Bool(true),
            "false or true;" to Bool(true),
            "false or false;" to Bool(false),
            "true and false;" to Bool(false),
            "true and true;" to Bool(true),
            "false and true;" to Bool(false),
            "false and false;" to Bool(false),
            "!false ;" to Bool(true),
            "!true ;" to Bool(false),

            // int relations
            "2 > 5;" to Bool(false),
            "2 < 5;" to Bool(true),
            "2 >= 5;" to Bool(false),
            "2 <= 5;" to Bool(true),
            "2 != 5;" to Bool(true),
            "2 == 5;" to Bool(false),
            "2 + 3 * 2 - 1 > 7;" to Bool(false),
            "2 + 3 * 2 - 1 < 7;" to Bool(false),
            "2 + 3 * 2 - 1 >= 7;" to Bool(true),
            "2 + 3 * 2 - 1 <= 7;" to Bool(true),
            "2 + 3 * 2 - 1 == 7;" to Bool(true),
            "2 + 3 * 2 - 1 != 7;" to Bool(false),

            // float relations
            "(2 + 3) * 2 - 1 > 7.0;" to Bool(true),
            "(2 + 3) / 5 == 1;" to Bool(true),
            "(2 + 3) / 10 >= 0.49;" to Bool(true),
            "(2 + 3) / 5 ==  5 -4 ;" to Bool (true),
            // str relations
            "\"hi\" == \"hi\";" to Bool(true),
            "\"hi\" == \"hi1\";" to Bool(false),
            "\"hi\" != \"hi1\";" to Bool(true),

            // bool relations
            "true == false;" to Bool(false),
            "false == true;" to Bool(false),
            "true == true;" to Bool(true),
            "false == false;" to Bool(true),
            "true != false;" to Bool(true),
            "false != true;" to Bool(true),
            "true != true;" to Bool(false),
            "false != false;" to Bool(false),
        )
        for ((query, expected) in tests) {
            val ast = Parser(query).parse()
            assertEquals(expected, ast.eval(), query)
        }
    }
    @Test
    fun testList() {
        val tests = mapOf(
            "[1];" to arrayListOf(Int64(1)),
            "[1.0];" to arrayListOf(Float(1.0)),
            "[1, \"hi\", true, false, 4.0];" to arrayListOf(
                Int64(1), Str("hi"), Bool(true), Bool(false), Float(4.0)
            ),
            "[1, \"hi\", true, false, 4.0, ];" to arrayListOf(
                Int64(1), Str("hi"), Bool(true), Bool(false), Float(4.0)
            ),
            "[1, [1, \"hi\"]];" to arrayListOf(
                Int64(1), List(arrayListOf(Int64(1), Str("hi")))
            ),
            "[];" to arrayListOf(),
        )
        for ((query, expected) in tests) {
            val ast = Parser(query).parse()
            assertEquals(List(expected as ArrayList<Value>), ast.eval(), query)
        }
    }
    @Test
    fun testDict() {
        val tests = mapOf(
            "{x=5,};" to hashMapOf(
                "x" to Int64(5),
            ),
            "{x=5,y=true, z=\"hi\", l=[1, 2], d = {yo=1}};" to hashMapOf(
                "x" to Int64(5),
                "y" to Bool(true),
                "z" to Str("hi"),
                "l" to List(arrayListOf(Int64(1), Int64(2))),
                "d" to Dict(hashMapOf("yo" to Int64(1))),
            ),
            "{};" to hashMapOf(),
        )
        for ((query, expected) in tests) {
            val ast = Parser(query).parse()
            assertEquals(Dict(expected as HashMap<String, Value>), ast.eval(), query)
        }
    }
    @Test
    fun testVar() {
        val tests = mapOf(
            "x = 5;" to Int64(5),
            "x = 5; \$x;" to Int64(5),
            "x = 3.4; \$x;" to Float(3.4),
            "x = 3.4; y = 5 > \$x;" to Bool(true),
            "x = 3.4; y = 5 + \$x;" to Float(8.4),
            "x = \"hi\"; \$x;" to Str("hi"),
            "x = {x=5,y=true, z=\"hi\", l=[1, 2], d = {yo=1,}}; \$x.x;" to Int64(5),
            "x = {x=5,y=true, z=\"hi\", l=[1, 2], d = {yo=1,}}; \$x.l[1];" to Int64(2),
            "x = {x=5,y=true, z=\"hi\", l=[1, 2], d = {yo=1,}}; \$x.d.yo;" to Int64(1),

            // multiple assignments
            "x = 3.4; y = 1 + \$x; z = \$x + \$y; \$z + 1;" to Float(8.8),
            "s = \"hi\"; x = {hi=[1, 2],}; \$x[\$s][1];" to Int64(2),
        )
        for ((query, expected) in tests) {
            val ast = Parser(query).parse()
            assertEquals(expected, ast.eval(), query)
        }
    }
}