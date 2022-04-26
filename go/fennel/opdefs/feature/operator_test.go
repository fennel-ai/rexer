package feature

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	feature2 "fennel/controller/feature"
	"fennel/kafka"
	"fennel/lib/feature"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
	"fennel/test/optest"
)

func TestFeatureLog_Apply(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := int64(1231231)
	clock.Set(t0)
	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        feature.KAFKA_TOPIC_NAME,
		GroupID:      "testgroup",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	f1 := value.NewDict(map[string]value.Value{"f1": value.Int(2), "f2": value.Double(1.0)})
	f2 := value.NewDict(map[string]value.Value{"f1": value.Int(3), "f2": value.Double(1.8)})
	static := value.NewDict(map[string]value.Value{"workflow": value.String("homefeed"), "model_name": value.String("mymodel"), "model_version": value.String("0.1.0")})
	inputs := []value.Value{
		value.NewDict(map[string]value.Value{"something": value.Bool(true), "b": value.Int(1)}),
		value.NewDict(map[string]value.Value{"something": value.Bool(false), "b": value.Int(4)}),
	}
	kwargs := []value.Dict{
		value.NewDict(map[string]value.Value{"context_otype": value.String("user"), "context_oid": value.String("1"), "candidate_otype": value.String("video"), "candidate_oid": value.String("723"), "request_id": value.String("1232"), "features": f1, "model_prediction": value.Double(0.59), "timestamp": value.Int(0)}),
		value.NewDict(map[string]value.Value{"context_otype": value.String("user"), "context_oid": value.String("2"), "candidate_otype": value.String("video"), "candidate_oid": value.String("823"), "request_id": value.String("1233"), "features": f2, "model_prediction": value.Double(0.79), "timestamp": value.Int(12312)}),
	}

	rows := []feature.Row{
		{
			ContextOType:    "user",
			ContextOid:      "1",
			CandidateOType:  "video",
			CandidateOid:    "723",
			Features:        f1,
			Workflow:        "homefeed",
			RequestID:       "1232",
			Timestamp:       ftypes.Timestamp(t0),
			ModelName:       "mymodel",
			ModelVersion:    "0.1.0",
			ModelPrediction: 0.59,
		},
		{
			ContextOType:    "user",
			ContextOid:      "2",
			CandidateOType:  "video",
			CandidateOid:    "823",
			Features:        f2,
			Workflow:        "homefeed",
			RequestID:       "1233",
			Timestamp:       ftypes.Timestamp(12312),
			ModelName:       "mymodel",
			ModelVersion:    "0.1.0",
			ModelPrediction: 0.79,
		},
	}
	outputs := make([]value.Value, len(rows))
	for i, r := range rows {
		outputs[i] = r.GetValue()
	}
	optest.AssertElementsMatch(t, tier, &featureLog{tier}, static, [][]value.Value{inputs}, kwargs, outputs)
	for _, r := range rows {
		rowptr, err := feature2.Read(context.Background(), consumer)
		assert.NoError(t, err)
		assert.Equal(t, r, *rowptr)
	}
}
