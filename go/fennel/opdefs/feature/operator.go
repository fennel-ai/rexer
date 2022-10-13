package feature

import (
	"context"
	"log"

	"fennel/controller/feature"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	libfeature "fennel/lib/feature"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var featurelog_stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "featurelog_stats",
	Help: "Stats about features being logged",
}, []string{"workflow"})

func init() {
	if err := operators.Register(featureLog{}); err != nil {
		log.Fatalf("Failed to register feature.log operator: %v", err)
	}
}

type featureLog struct {
	tier tier.Tier
}

func (f featureLog) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return featureLog{tr}, nil
}

func (f featureLog) Apply(ctx context.Context, static operators.Kwargs, in operators.InputIter, out *value.List) error {
	workflow := string(static.GetUnsafe("workflow").(value.String))
	modelName := ftypes.ModelName(static.GetUnsafe("model_name").(value.String))
	modelVersion := ftypes.ModelVersion(static.GetUnsafe("model_version").(value.String))
	featurelog_entries := 0
	for in.HasMore() {
		_, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		ts := ftypes.Timestamp(kwargs.GetUnsafe("timestamp").(value.Int))
		if ts == 0 {
			ts = ftypes.Timestamp(f.tier.Clock.Now().Unix())
		}
		msg := libfeature.Row{
			ContextOType:    ftypes.OType(kwargs.GetUnsafe("context_otype").(value.String)),
			ContextOid:      ftypes.OidType(kwargs.GetUnsafe("context_oid").String()),
			CandidateOType:  ftypes.OType(kwargs.GetUnsafe("candidate_otype").(value.String)),
			CandidateOid:    ftypes.OidType(kwargs.GetUnsafe("candidate_oid").String()),
			Features:        kwargs.GetUnsafe("features").(value.Dict),
			Workflow:        workflow,
			RequestID:       ftypes.RequestID(kwargs.GetUnsafe("request_id").String()),
			Timestamp:       ts,
			ModelName:       modelName,
			ModelVersion:    modelVersion,
			ModelPrediction: float64(kwargs.GetUnsafe("model_prediction").(value.Double)),
		}
		if err = feature.Log(ctx, f.tier, msg); err != nil {
			return err
		}
		row, err := msg.GetValue()
		if err != nil {
			return err
		}
		featurelog_entries++
		out.Append(row)
	}
	featurelog_stats.WithLabelValues(workflow).Set(float64(featurelog_entries))
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
