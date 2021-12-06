package starql

class LexException(private val msg: String) : Exception(msg)
class ParseException(private val msg: String) : Exception(msg)
class EvalException(private val msg: String) : RuntimeException(msg)