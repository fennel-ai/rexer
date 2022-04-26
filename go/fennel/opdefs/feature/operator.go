package feature

import (
	"context"
	"sync"

	"fennel/controller/feature"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	libfeature "fennel/lib/feature"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	operators.Register(featureLog{})
}

type featureLog struct {
	tier tier.Tier
}

func (f featureLog) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return featureLog{tr}, nil
}

func (f featureLog) Apply(ctx context.Context, static value.Dict, in operators.InputIter, out *value.List) error {
	workflow := string(get(static, "workflow").(value.String))
	modelName := ftypes.ModelName(get(static, "model_name").(value.String))
	modelVersion := ftypes.ModelVersion(get(static, "model_version").(value.String))

	for in.HasMore() {
		_, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		ts := ftypes.Timestamp(get(kwargs, "timestamp").(value.Int))
		if ts == 0 {
			ts = ftypes.Timestamp(f.tier.Clock.Now())
		}
		msg := libfeature.Row{
			ContextOType:    ftypes.OType(get(kwargs, "context_otype").(value.String)),
			ContextOid:      ftypes.OidType(get(kwargs, "context_oid").String()),
			CandidateOType:  ftypes.OType(get(kwargs, "candidate_otype").(value.String)),
			CandidateOid:    ftypes.OidType(get(kwargs, "candidate_oid").String()),
			Features:        get(kwargs, "features").(value.Dict),
			Workflow:        workflow,
			RequestID:       ftypes.RequestID(get(kwargs, "request_id").String()),
			Timestamp:       ts,
			ModelName:       modelName,
			ModelVersion:    modelVersion,
			ModelPrediction: float64(get(kwargs, "model_prediction").(value.Double)),
		}
		if err = feature.Log(ctx, f.tier, msg); err != nil {
			return err
		}
		row, err := msg.GetValue()
		if err != nil {
			return err
		}
		out.Append(row)
	}
	return nil
}

func (f featureLog) Signature() *operators.Signature {
	return operators.NewSignature("feature", "log").
		Input(nil).
		Param("context_otype", value.Types.String, false, false, value.Nil).
		Param("context_oid", value.Types.ID, false, false, value.Nil).
		Param("candidate_otype", value.Types.String, false, false, value.Nil).
		Param("candidate_oid", value.Types.ID, false, false, value.Nil).
		Param("features", value.Types.Dict, false, false, value.Nil).
		Param("workflow", value.Types.String, true, false, value.Nil).
		Param("request_id", value.Types.ID, false, false, value.Nil).
		Param("model_name", value.Types.String, true, true, value.String("")).
		Param("model_version", value.Types.String, true, true, value.String("")).
		Param("model_prediction", value.Types.Double, false, true, value.Double(-1)).
		Param("timestamp", value.Types.Int, false, true, value.Int(0))
}

var _ operators.Operator = &featureLog{}

func get(d value.Dict, k string) value.Value {
	ret, _ := d.Get(k)
	return ret
}
