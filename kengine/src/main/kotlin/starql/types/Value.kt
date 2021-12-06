package starql.types

import starql.EvalException

sealed class Value : Comparable<Value> {
    operator fun plus(other: Value): Value {
        return when {
            this is Float && other is Float -> Float(n + other.n)
            this is Int64 && other is Int64 -> Int64(n + other.n)
            this is Float && other is Int64 -> Float(n + other.n)
            this is Int64 && other is Float -> Float(n + other.n)
            this is Str && other is Str -> Str(this.s + other.s)
            this is List && other is List -> List(ArrayList(this.l + other.l))
            else -> throw EvalException("plus operator only supported for numbers, strings, and lists")
        }
    }

    operator fun minus(other: Value): Value {
        return when {
            this is Float && other is Float -> Float(n - other.n)
            this is Float && other is Int64 -> Float(n - other.n)
            this is Int64 && other is Float -> Float(n - other.n)
            this is Int64 && other is Int64 -> Int64(n - other.n)
            else -> throw EvalException("minus operator only supported for numbers")
        }
    }

    operator fun times(other: Value): Value {
        return when {
            this is Float && other is Float -> Float(n * other.n)
            this is Int64 && other is Int64 -> Int64(n * other.n)
            this is Float && other is Int64 -> Float(n * other.n)
            this is Int64 && other is Float -> Float(n * other.n)
            else -> throw EvalException("times operator only supported for numbers")
        }
    }

    operator fun div(other: Value): Value {
        return when {
            this is Float && other is Float -> {
                try {
                    Float(n / other.n)
                } catch (e: ArithmeticException) {
                    throw EvalException("division br zero")
                }
            }
            this is Int64 && other is Int64 -> {
                try {
                    Float(1.0 * n / other.n)
                } catch (e: ArithmeticException) {
                    throw EvalException("division br zero")
                }
            }
            this is Int64 && other is Float -> {
                try {
                    Float(1.0 * n / other.n)
                } catch (e: ArithmeticException) {
                    throw EvalException("division br zero")
                }
            }
            this is Float && other is Int64 -> {
                try {
                    Float(1.0 * n / other.n)
                } catch (e: ArithmeticException) {
                    throw EvalException("division br zero")
                }
            }
            else -> throw EvalException("div operator only supported for numbers")
        }
    }

    operator fun unaryMinus(): Value {
        return when (this) {
            is Float -> Float(-n)
            is Int64 -> Int64(-n)
            else -> throw EvalException("unary minus only supported for numbers")
        }
    }

    operator fun not(): Value {
        return when (this) {
            is Bool -> Bool(!this.b)
            else -> throw EvalException("not operator only supported for booleans")
        }
    }

    fun and(other: Value): Value {
        return when {
            this is Bool && other is Bool -> Bool(this.b && other.b)
            else -> throw EvalException("arguments to && operator should be booleans")
        }
    }

    fun or(other: Value): Value {
        return when {
            this is Bool && other is Bool -> Bool(this.b || other.b)
            else -> throw EvalException("arguments to || operator should be booleans")
        }
    }

    override fun compareTo(other: Value): Int {
        return when {
            this is Float && other is Float -> this.n.compareTo(other.n)
            this is Int64 && other is Int64 -> this.n.compareTo(other.n)
            this is Float && other is Int64 -> this.n.compareTo(other.n)
            this is Int64 && other is Float -> this.n.compareTo(other.n)
            else -> throw EvalException("comparison only supported for numbers")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (other !is Value) {
            throw EvalException("values can only be compared with values")
        }
        return when {
            this is Float && other is Float -> this.n.equals(other.n)
            this is Int64 && other is Int64 -> this.n == other.n
            this is Float && other is Int64 -> this.n.equals(other.n.toDouble())
            this is Int64 && other is Float -> other.n.equals(this.n.toDouble())
            this is Str && other is Str -> this.s == other.s
            this is Bool && other is Bool -> this.b == other.b
            this is List && other is List -> this.l == other.l
            this is Dict && other is Dict -> this.m == other.m
            else -> throw EvalException("comparison only supported for same type values")
        }
    }

    override fun toString(): String {
        return when (this) {
            is Float -> "Float(${this.n})"
            is Int64 -> "Int64(${this.n})"
            is Str -> "Str(${this.s})"
            is Bool -> "Bool(${this.b})"
            is List -> "List(${this.l})"
            is Dict -> "Dict(${this.m})"
        }
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }
}

class Float(val n: Double) : Value()
class Int64(val n: Int) : Value()
class Str(val s: String) : Value()
class Bool(val b: Boolean) : Value()
class List(val l: ArrayList<Value>) : Value()
class Dict(val m: HashMap<String, Value>) : Value()