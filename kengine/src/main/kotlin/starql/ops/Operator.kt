package starql.ops

import starql.EvalException
import starql.types.Value


typealias Parameters = HashMap<String, (Value) -> Value>
typealias Args = HashMap<String, Value>

class ParamDef(val isLocal: Boolean, val default: Value?)


abstract class Operator {
    // Every inheriting class has to override these things
    abstract val module: String
    abstract val name: String
    abstract val params: HashMap<String, ParamDef>
    abstract fun apply(outData: ArrayList<Value>)

    // everything below this is "final" and can not be overridden
    private var inData: ArrayList<Value>? = null
    private var staticParams: Args? = null
    private var localParams: Parameters? = null
    private val batch = 128
    private var idx = 0

    init {
        // TODO: find a way of enabling this so that operator writers don't have to manually register
        // Registry.set(module, name, ::class)
    }

    fun init(inData: ArrayList<Value>, staticParams: Args, localParams: Parameters) {
        this.inData = inData
        this.staticParams = staticParams
        this.localParams = localParams
    }

    fun paramIsLocal(paramName: String): Boolean? {
        if (paramName !in params) {
            return null
        }
        return params[paramName]!!.isLocal
    }

    fun getStaticParams(): Args {
        if (staticParams == null) {
            throw EvalException("applying operator $this without initing")
        }
        return staticParams!!
    }

    fun isEmpty(): Boolean {
        if (inData == null) {
            throw EvalException("applying operator $this without initing")
        }
        return idx >= inData!!.size
    }

    fun pull_single(): Pair<Value, Args>? {
        if (inData == null || localParams == null) {
            throw EvalException("applying operator $this without initing")
        }
        if (isEmpty()) {
            return null
        }
        val data = inData!![idx]
        val args = hashMapOf<String, Value>()
        for ((n, f) in localParams!!) {
            args[n] = f(data)
        }
        idx += 1
        return (data to args)
    }

    fun pull(all: Boolean = false): ArrayList<Pair<Value, Args>> {
        val ret = arrayListOf<Pair<Value, Args>>()
        val range = if (all) 1..Int.MAX_VALUE else 1..batch
        for (i in range) {
            when (isEmpty()) {
                true -> break
                else -> ret.add(pull_single()!!)
            }
        }
        return ret
    }

}
