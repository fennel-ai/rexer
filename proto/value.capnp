using Go = import "/go.capnp";

@0x8cfb90d81c574cef;
$Go.package("value");
$Go.import("fennel/lib/value");

struct Map(K, V) {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :K;
    value @1 :V;
  }
}

struct CapnValue {
    union {
        int @0 :Int64;
        double @1 :Float64;
        bool @2 :Bool;
        str @3 :Text;
        list @4 :List(CapnValue);
        dict @5 :Map(Text, CapnValue);
        table @6 :List(Map(Text, CapnValue));
        nil @7 :Void;
    }
}
