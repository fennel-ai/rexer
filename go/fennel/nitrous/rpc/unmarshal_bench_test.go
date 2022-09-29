package rpc

import (
	"capnproto.org/go/capnp/v3"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fmt"
	"github.com/golang/protobuf/proto"
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

	fmt.Printf("len: %s: %d\n", b.Name(), l)

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

	fmt.Printf("len: %s: %d\n", b.Name(), l)

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

	fmt.Printf("len: %s %d\n", b.Name(), l)

	// reset to not report setup time
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := captainUnmarshal(ops); err != nil {
			panic(err)
		}
	}
}

/*
BenchmarkUnmarshal/oneof-10
     315	   3764591 ns/op	 2400025 B/op	   60000 allocs/op
len: BenchmarkUnmarshal/oneof: 269702
BenchmarkUnmarshal/simple-10
     405	   2959862 ns/op	 2320016 B/op	   50000 allocs/op
len: BenchmarkUnmarshal/simple: 269702
BenchmarkUnmarshal/captain-simple-10
    1227	    984746 ns/op	 1920005 B/op	   30000 allocs/op
len: BenchmarkUnmarshal/captain-simple 960000
*/
func BenchmarkUnmarshal(b *testing.B) {
	b.Run("oneof", benchmarkOneof)
	b.Run("simple", benchmarkSimple)
	b.Run("captain-simple", benchmarkCaptain)
}