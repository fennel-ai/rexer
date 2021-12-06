package starql.interpreter

import starql.EvalException
import starql.lexer.Token
import starql.types.Value

class Environment(private val parent: Environment?) {
    private val map = HashMap<String, Value>()

    fun define(k: Token, v: Value) {
        val ks = k.literal()
        if (ks in map) {
            throw EvalException("cannot redefine variable $k")
        }
        map[ks] = v
    }

    fun get(k: Token): Value? {
        val ks = k.literal()
        return if (ks in map) map[ks] else parent?.get(k)
    }
}