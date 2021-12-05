package starql.types

import starql.EvalException

sealed class Value : Comparable<Value> {
    operator fun plus(other: Value): Value {
        return when {
            this is Num && other is Num -> Num(this.n + other.n)
            this is Str && other is Str -> Str(this.s + other.s)
            this is List && other is List -> List(ArrayList(this.l + other.l))
            else -> throw EvalException("plus operator only supported for numbers, strings, and lists")
        }
    }

    operator fun minus(other: Value): Value {
        return when {
            this is Num && other is Num -> Num(this.n - other.n)
            else -> throw EvalException("minus operator only supported for numbers")
        }
    }

    operator fun times(other: Value): Value {
        return when {
            this is Num && other is Num -> Num(this.n * other.n)
            else -> throw EvalException("times operator only supported for numbers")
        }
    }

    operator fun div(other: Value): Value {
        return when {
            this is Num && other is Num -> {
                try {
                    @Suppress("DIVISION_BY_ZERO")
                    Num(this.n / other.n)
                } catch (e: ArithmeticException) {
                    throw EvalException("division br zero")
                }
            }
            else -> throw EvalException("div operator only supported for numbers")
        }
    }

    operator fun unaryMinus(): Value {
        return when (this) {
            is Num -> Num(-this.n)
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
            this is Num && other is Num -> this.n.compareTo(other.n)
            else -> throw EvalException("comparison only supported for numbers")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (other !is Value) {
            throw EvalException("values can only be compared with values")
        }
        return when {
            this is Num && other is Num -> this.n.equals(other.n)
            this is Str && other is Str -> this.s == other.s
            this is Bool && other is Bool -> this.b == other.b
            this is List && other is List -> this.l == other.l
            else -> throw EvalException("comparison only supported for same type values")
        }
    }

    override fun toString(): String {
        return when (this) {
            is Num -> "Num(${this.n})"
            is Str -> "Str(${this.s})"
            is Bool -> "Bool(${this.b})"
            is List -> "List(${this.l})"
        }
    }
}

class Num(val n: Double) : Value()
class Str(val s: String) : Value()
class Bool(val b: Boolean) : Value()
class List(val l: ArrayList<Value>) : Value()