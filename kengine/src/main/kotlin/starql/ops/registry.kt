package starql.ops

object Registry {
    private val map = HashMap<String, HashMap<String, () -> Operator>>()

    init {
        set("std", "filter", ::Filter)
        set("std", "first", ::First)
        set("std", "last", ::Last)
        set("std", "take", ::Take)
    }

    fun get(module: String, name: String): Operator? {
        val klass = map[module]?.get(name)
        return if (klass == null) null else klass()
    }

    fun set(module: String, name: String, klass: () -> Operator) {
        if (module !in map) {
            map[module] = HashMap()
        }
        map[module]!![name] = klass
    }
}
