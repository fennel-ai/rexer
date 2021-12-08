package starql.ops

import starql.EvalException
import starql.types.Bool
import starql.types.Int64
import starql.types.Value

class Filter : Operator() {
    override val module = "std"
    override val name = "filter"
    override val params = hashMapOf(
        "where" to ParamDef(true, Bool(true)),
    )

    override fun apply(outData: ArrayList<Value>) {
        while (!isEmpty()) {
            for ((row, args) in pull()) {
                if (args["where"]!! == Bool(true)) {
                    outData.add(row)
                }
            }
        }
    }
}

class First : Operator() {
    override val module = "std"
    override val name = "first"
    override val params = hashMapOf<String, ParamDef>()

    override fun apply(outData: ArrayList<Value>) {
        if (isEmpty()) {
            throw EvalException("Can not take first element from an empty list")
        }
        val (row, _) = pull_single()!!
        outData.add(row)
    }
}

class Last : Operator() {
    override val module = "std"
    override val name = "last"
    override val params = hashMapOf<String, ParamDef>()

    override fun apply(outData: ArrayList<Value>) {
        if (isEmpty()) {
            throw EvalException("Can not take last element from an empty list")
        }
        var last: Value? = null
        while (!isEmpty()) {
            val (row, _) = pull_single()!!
            last = row
        }
        outData.add(last!!)
    }
}

class Take : Operator() {
    override val module = "std"
    override val name = "take"
    override val params = hashMapOf(
        "limit" to ParamDef(false, null)
    )

    override fun apply(outData: ArrayList<Value>) {
        var limit = getStaticParams()["limit"] as Int64
        var taken = 0
        while (!isEmpty() && taken < limit.n) {
            val (row, _) = pull_single()!!
            outData.add(row)
            taken += 1
        }
    }
}
