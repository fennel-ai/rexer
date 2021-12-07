package starql.interpreter

import starql.EvalException
import starql.types.Value

class Environment(private val parent: Environment?) {
    private val map = HashMap<String, Value>()

    fun define(k: String, v: Value) {
        if (k in map) {
            throw EvalException("cannot redefine variable $k")
        }
        map[k] = v
    }

    fun get(k: String): Value? {
        return if (k in map) map[k] else parent?.get(k)
    }
}