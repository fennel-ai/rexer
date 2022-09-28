using Go = import "/go.capnp";

@0x896ac3e20ff6ad0c;
$Go.package("rpc");
$Go.import("fennel/nitrous/rpc");

struct AggEventCap {
    aggId @0 : UInt32;
    groupkey @1 : Text;
    timestamp @2 : UInt32;
}

struct NitrousBinlogEventCap {
    tierId @0 : UInt32;
    aggEvent @1 : AggEventCap;
}