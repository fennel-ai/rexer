package rpc

import (
	"capnproto.org/go/capnp/v3"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"testing"
)


func nitrousOpUnmarshal(ops [][]byte) error {
	for _, op := range ops {
		o := NitrousOp{}
		if err := proto.Unmarshal(op, &o); err != nil {
			return err
		}
	}
	return nil
}

func nitrousSimpleOpUnmarshal(ops [][]byte) error {
	for _, op := range ops {
		o := NitrousBinlogEvent{}
		if err := proto.Unmarshal(op, &o); err != nil {
			return err
		}
	}
	return nil
}

func captainUnmarshal(ops [][]byte) error {
	for _, op := range ops {
		if _, err := capnp.Unmarshal(op); err != nil {
			return err
		}
	}
	return nil
}

func captainUnmarshalPacked(ops [][]byte) error {
	for _, op := range ops {
		if _, err := capnp.UnmarshalPacked(op); err != nil {
			return err
		}
	}
	return nil
}

func captainUnmarshalCompressed(ops [][]byte) error {
	for _, op := range ops {
		d := make([]byte, 100 * len(op))
		n, err := lz4.UncompressBlock(op, d)
		if err != nil {
			return err
		}
		if _, err := capnp.Unmarshal(d[:n]); err != nil {
			return err
		}
	}
	return nil
}

func benchmarkOneof(b *testing.B) {
	b.ReportAllocs()
	ops := make([][]byte, 10_000)
	actual := make([]*NitrousOp, 10_000)
	l := 0
	for i := 0; i < 10_000; i++ {
		v := value.Int(10)
		pv, err := value.ToProtoValue(v)
		assert.NoError(b, err)
		op := NitrousOp{
			TierId: uint32(i),
			Type:   OpType_AGG_EVENT,
			Op: &NitrousOp_AggEvent{
				AggEvent: &AggEvent{
					AggId:     uint32(21),
					Groupkey:  utils.RandString(10),
					Value:     &pv,
					Timestamp: uint32(i * 100),
				},
			},
		}
		data, err := proto.Marshal(&op)
		assert.NoError(b, err)
		ops[i] = data
		l += len(data)
		actual[i] = &op
	}

	b.Logf("data: %d", l)

	// reset to not report setup time
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := nitrousOpUnmarshal(ops); err != nil {
			panic(err)
		}
	}
}

func benchmarkSimple(b *testing.B) {
	b.ReportAllocs()
	ops := make([][]byte, 10_000)
	actual := make([]*NitrousBinlogEvent, 10_000)
	l := 0
	for i := 0; i < 10_000; i++ {
		v := value.Int(10)
		pv, err := value.ToProtoValue(v)
		assert.NoError(b, err)
		op := NitrousBinlogEvent{
			TierId: uint32(i),
			AggEvent: &AggEvent{
				AggId:     uint32(21),
				Groupkey:  utils.RandString(10),
				Value:     &pv,
				Timestamp: uint32(i * 100),
			},
		}
		data, err := proto.Marshal(&op)
		assert.NoError(b, err)
		ops[i] = data
		l += len(data)
		actual[i] = &op
	}

	b.Logf("data: %d", l)
	// reset to not report setup time
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := nitrousSimpleOpUnmarshal(ops); err != nil {
			panic(err)
		}
	}
}

func benchmarkCaptain(b *testing.B) {
	b.ReportAllocs()
	ops := make([][]byte, 10_000)
	actual := make([]*NitrousBinlogEventCap, 10_000)
	l := 0
	for i := 0; i < 10_000; i++ {
		msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
		assert.NoError(b, err)
		cv, err := NewRootNitrousBinlogEventCap(seg)
		cv.SetTierId(uint32(i))

		_, s, err := capnp.NewMessage(capnp.SingleSegment(nil))
		aggEvent, err := NewAggEventCap(s)
		aggEvent.SetAggId(uint32(21))
		err = aggEvent.SetGroupkey(utils.RandString(10))
		assert.NoError(b, err)
		aggEvent.SetTimestamp(uint32(i * 100))
		v := value.Int(10)
		capv, _, err := value.ToCapnValue(v)
		assert.NoError(b, err)
		err = aggEvent.SetValue(capv)
		assert.NoError(b, err)

		err = cv.SetAggEvent(aggEvent)
		assert.NoError(b, err)

		data, err := msg.Marshal()
		assert.NoError(b, err)
		ops[i] = data
		l += len(data)
		actual[i] = &cv
	}

	b.Logf("data: %d", l)

	// reset to not report setup time
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := captainUnmarshal(ops); err != nil {
			panic(err)
		}
	}
}

func benchmarkCaptainCompression(b *testing.B) {
	b.ReportAllocs()
	ops := make([][]byte, 10_000)
	actual := make([]*NitrousBinlogEventCap, 10_000)
	var compressor lz4.Compressor
	l := 0
	for i := 0; i < 10_000; i++ {
		msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
		assert.NoError(b, err)
		cv, err := NewRootNitrousBinlogEventCap(seg)
		cv.SetTierId(uint32(i))

		_, s, err := capnp.NewMessage(capnp.SingleSegment(nil))
		aggEvent, err := NewAggEventCap(s)
		aggEvent.SetAggId(uint32(21))
		err = aggEvent.SetGroupkey(utils.RandString(10))
		assert.NoError(b, err)
		aggEvent.SetTimestamp(uint32(i * 100))
		v := value.Int(10)
		capv, _, err := value.ToCapnValue(v)
		assert.NoError(b, err)
		err = aggEvent.SetValue(capv)
		assert.NoError(b, err)

		err = cv.SetAggEvent(aggEvent)
		assert.NoError(b, err)

		data, err := msg.Marshal()
		d := make([]byte, lz4.CompressBlockBound(len(data)))
		n, err := compressor.CompressBlock(data, d)
		assert.NoError(b, err)
		if n > len(data) {
			fmt.Printf("%v not compressable\n", data)
		}
		ops[i] = d[:n]
		l += n
		actual[i] = &cv
	}

	b.Logf("data: %d", l)

	// reset to not report setup time
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := captainUnmarshalCompressed(ops); err != nil {
			panic(err)
		}
	}
}


func benchmarkCaptainPacked(b *testing.B) {
	b.ReportAllocs()
	ops := make([][]byte, 10_000)
	actual := make([]*NitrousBinlogEventCap, 10_000)
	l := 0
	for i := 0; i < 10_000; i++ {
		msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
		assert.NoError(b, err)
		cv, err := NewRootNitrousBinlogEventCap(seg)
		cv.SetTierId(uint32(i))

		_, s, err := capnp.NewMessage(capnp.SingleSegment(nil))
		aggEvent, err := NewAggEventCap(s)
		aggEvent.SetAggId(uint32(21))
		err = aggEvent.SetGroupkey(utils.RandString(10))
		assert.NoError(b, err)
		aggEvent.SetTimestamp(uint32(i * 100))
		v := value.Int(10)
		capv, _, err := value.ToCapnValue(v)
		assert.NoError(b, err)
		err = aggEvent.SetValue(capv)
		assert.NoError(b, err)

		err = cv.SetAggEvent(aggEvent)
		assert.NoError(b, err)

		data, err := msg.MarshalPacked()
		assert.NoError(b, err)
		ops[i] = data
		l += len(data)
		actual[i] = &cv
	}

	b.Logf("data: %d", l)

	// reset to not report setup time
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := captainUnmarshalPacked(ops); err != nil {
			panic(err)
		}
	}
}

/*
BenchmarkUnmarshal/oneof-10         	     326	   3624104 ns/op	 2400027 B/op	   60000 allocs/op
--- BENCH: BenchmarkUnmarshal/oneof-10
    unmarshal_bench_test.go:95: data: 269702
    unmarshal_bench_test.go:95: data: 269702
    unmarshal_bench_test.go:95: data: 269702
BenchmarkUnmarshal/simple-10        	     420	   2857430 ns/op	 2320017 B/op	   50000 allocs/op
--- BENCH: BenchmarkUnmarshal/simple-10
    unmarshal_bench_test.go:131: data: 269702
    unmarshal_bench_test.go:131: data: 269702
    unmarshal_bench_test.go:131: data: 269702
BenchmarkUnmarshal/captain-simple-10         	    1262	    971727 ns/op	 1920009 B/op	   30000 allocs/op
--- BENCH: BenchmarkUnmarshal/captain-simple-10
    unmarshal_bench_test.go:174: data: 960000
    unmarshal_bench_test.go:174: data: 960000
    unmarshal_bench_test.go:174: data: 960000
BenchmarkUnmarshal/captain-simple-compression-10         	     129	   8350849 ns/op	96640642 B/op	   40006 allocs/op
--- BENCH: BenchmarkUnmarshal/captain-simple-compression-10
    unmarshal_bench_test.go:224: data: 869325
    unmarshal_bench_test.go:224: data: 869321
    unmarshal_bench_test.go:224: data: 869325
BenchmarkUnmarshal/captain-packed-10                     	     427	   2795395 ns/op	 4400030 B/op	   80000 allocs/op
--- BENCH: BenchmarkUnmarshal/captain-packed-10
    unmarshal_bench_test.go:269: data: 398852
    unmarshal_bench_test.go:269: data: 398852
    unmarshal_bench_test.go:269: data: 398852
*/
func BenchmarkUnmarshal(b *testing.B) {
	b.Run("oneof", benchmarkOneof)
	b.Run("simple", benchmarkSimple)
	b.Run("captain-simple", benchmarkCaptain)
	b.Run("captain-simple-compression", benchmarkCaptainCompression)
	b.Run("captain-packed", benchmarkCaptainPacked)
	// b.Run("captain-packed-compression", benchmarkCaptainPackedCompression)
	// skipping compression because it is an add-on to packing - since with packing we see slower ops, compression
	// will likely reduce it further
	//
	// also it fails here with the condition -
	//
	// n, err := compressor.CompressBlock(data, d)
	//		assert.NoError(b, err)
	//
	//		 --- This is triggered ---
	//		if n > len(data) {
	//			fmt.Printf("%v not compressable\n", data)
	//		}
}