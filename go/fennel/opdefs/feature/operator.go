package feature

import (
	"context"

	"fennel/controller/feature"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	libfeature "fennel/lib/feature"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	operators.Register(&featureLog{})
}

type featureLog struct {
	tier tier.Tier
}

func (f *featureLog) Init(args value.Dict, bootargs map[string]interface{}) error {
	var err error
	if f.tier, err = bootarg.GetTier(bootargs); err != nil {
		return err
	}
	return nil
}

func (f *featureLog) Apply(static value.Dict, in operators.InputIter, out *value.List) error {
	contextOtype := ftypes.OType(static["context_otype"].(value.String))
	contextOid := ftypes.OidType(static["context_oid"].(value.Int))
	workflow := string(static["workflow"].(value.String))
	requestID := ftypes.RequestID(static["request_id"].(value.Int))
	modelID := ftypes.ModelID(static["model_id"].(value.String))

	for in.HasMore() {
		row, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		ts := ftypes.Timestamp(kwargs["timestamp"].(value.Int))
		if ts == 0 {
			ts = ftypes.Timestamp(f.tier.Clock.Now())
		}
		msg := libfeature.Row{
			ContextOType:    contextOtype,
			ContextOid:      contextOid,
			CandidateOType:  ftypes.OType(kwargs["candidate_otype"].(value.String)),
			CandidateOid:    ftypes.OidType(kwargs["candidate_oid"].(value.Int)),
			Features:        kwargs["features"].(value.Dict),
			Workflow:        workflow,
			RequestID:       requestID,
			Timestamp:       ts,
			ModelID:         modelID,
			ModelPrediction: float64(kwargs["model_prediction"].(value.Double)),
		}
		if err = feature.Log(context.TODO(), f.tier, msg); err != nil {
			return err
		}
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (f *featureLog) Signature() *operators.Signature {
	return operators.NewSignature("feature", "log", true).
		Input(value.Types.Dict).
		Param("context_otype", value.Types.String, true, false, value.Nil).
		Param("context_oid", value.Types.Int, true, false, value.Nil).
		Param("candidate_otype", value.Types.String, false, false, value.Nil).
		Param("candidate_oid", value.Types.Int, false, false, value.Nil).
		Param("features", value.Types.Dict, false, false, value.Nil).
		Param("workflow", value.Types.String, true, false, value.Nil).
		Param("request_id", value.Types.Int, true, false, value.Nil).
		Param("model_id", value.Types.String, true, true, value.String("")).
		Param("model_prediction", value.Types.Double, false, true, value.Double(-1)).
		Param("timestamp", value.Types.Int, false, true, value.Int(0))
}

var _ operators.Operator = &featureLog{}
