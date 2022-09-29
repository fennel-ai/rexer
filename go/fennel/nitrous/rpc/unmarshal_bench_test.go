package rpc

import (
	"capnproto.org/go/capnp/v3"
	"fennel/lib/utils"
	"fennel/lib/value"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"testing"
)


func benchmarkOneof(b *testing.B) {
	b.ReportAllocs()
	ops := make([][]byte, 10_000)
	actual := make([]*NitrousOp, 10_000)
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
		actual[i] = &op
	}

	// reset to not report setup time
	b.ResetTimer()

	b.StartTimer()
	parsedOps := make([]*NitrousOp, 10_000)
	for i := 0; i < 10_000; i++ {
		op := NitrousOp{}
		err := proto.Unmarshal(ops[i], &op)
		assert.NoError(b, err)
		parsedOps[i] = &op
	}
	b.StopTimer()

	// assert
	for i, d := range parsedOps {
		assert.EqualValues(b, d.GetAggEvent().String(), actual[i].GetAggEvent().String())
	}
}

func benchmarkSimple(b *testing.B) {
	b.ReportAllocs()
	ops := make([][]byte, 10_000)
	actual := make([]*NitrousBinlogEvent, 10_000)
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
		actual[i] = &op
	}

	// reset to not report setup time
	b.ResetTimer()

	b.StartTimer()
	parsedOps := make([]*NitrousBinlogEvent, 10_000)
	for i := 0; i < 10_000; i++ {
		op := &NitrousBinlogEvent{}
		err := proto.Unmarshal(ops[i], op)
		assert.NoError(b, err)
		parsedOps[i] = op
	}
	b.StopTimer()

	// assert
	for i, d := range parsedOps {
		assert.EqualValues(b, d.GetAggEvent().String(), actual[i].GetAggEvent().String())
	}
}

func benchmarkCaptain(b *testing.B) {
	b.ReportAllocs()
	ops := make([][]byte, 10_000)
	actual := make([]*NitrousBinlogEventCap, 10_000)
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
		err = cv.SetAggEvent(aggEvent)
		assert.NoError(b, err)

		data, err := msg.Marshal()
		assert.NoError(b, err)
		ops[i] = data
		actual[i] = &cv
	}

	// reset to not report setup time
	b.ResetTimer()

	b.StartTimer()
	parsedOps := make([]*NitrousBinlogEventCap, 10_000)
	for i := 0; i < 10_000; i++ {
		msg, err := capnp.Unmarshal(ops[i])
		assert.NoError(b, err)
		x, err := ReadRootNitrousBinlogEventCap(msg)
		assert.NoError(b, err)
		parsedOps[i] = &x
	}
	b.StopTimer()

	// assert
	for i, d := range parsedOps {
		assert.EqualValues(b, d.String(), actual[i].String())
	}
}

/*
BenchmarkUnmarshal/oneof-10         	1000000000	         0.003835 ns/op	       0 B/op	       0 allocs/op
BenchmarkUnmarshal/simple-10        	1000000000	         0.003041 ns/op	       0 B/op	       0 allocs/op
BenchmarkUnmarshal/captain-simple-10         	1000000000	         0.002221 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkUnmarshal(b *testing.B) {
	b.Run("oneof", benchmarkOneof)
	b.Run("simple", benchmarkSimple)
	b.Run("captain-simple", benchmarkCaptain)
}