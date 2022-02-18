package feature

import (
	"context"
	feature2 "fennel/controller/feature"
	"fennel/lib/feature"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
	"fennel/test/optest"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFeatureLog_Apply(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := int64(1231231)
	clock.Set(t0)

	f1 := value.Dict{"f1": value.Int(2), "f2": value.Double(1.0)}
	f2 := value.Dict{"f1": value.Int(3), "f2": value.Double(1.8)}
	static := value.Dict{"context_otype": value.String("user"), "context_oid": value.Int(1), "workflow": value.String("homefeed"), "request_id": value.Int(1232), "model_id": value.String("mymodel")}
	inputs := []value.Dict{{"something": value.Bool(true), "b": value.Int(1)}, {"something": value.Bool(false), "b": value.Int(4)}}
	kwargs := []value.Dict{
		{"candidate_otype": value.String("video"), "candidate_oid": value.Int(723), "features": f1, "model_prediction": value.Double(0.59), "timestamp": value.Int(0)},
		{"candidate_otype": value.String("video"), "candidate_oid": value.Int(823), "features": f2, "model_prediction": value.Double(0.79), "timestamp": value.Int(12312)},
	}

	rows := []feature.Row{
		{
			ContextOType:    "user",
			ContextOid:      1,
			CandidateOType:  "video",
			CandidateOid:    723,
			Features:        f1,
			Workflow:        "homefeed",
			RequestID:       1232,
			Timestamp:       ftypes.Timestamp(t0),
			ModelID:         "mymodel",
			ModelPrediction: 0.59,
		},
		{
			ContextOType:    "user",
			ContextOid:      1,
			CandidateOType:  "video",
			CandidateOid:    823,
			Features:        f2,
			Workflow:        "homefeed",
			RequestID:       1232,
			Timestamp:       ftypes.Timestamp(12312),
			ModelID:         "mymodel",
			ModelPrediction: 0.79,
		},
	}
	optest.Assert(t, featureLog{tier}, static, inputs, kwargs, inputs)
	for _, r := range rows {
		rowptr, err := feature2.Read(context.TODO(), tier)
		assert.NoError(t, err)
		assert.Equal(t, r, *rowptr)
	}
}
