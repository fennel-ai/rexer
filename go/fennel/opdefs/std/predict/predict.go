package predict

import (
	"context"

	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/sagemaker"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	if err := operators.Register(predictOperator{}); err != nil {
		panic(err)
	}
}

type predictOperator struct {
	tier tier.Tier
}

var _ operators.Operator = predictOperator{}

func (pop predictOperator) New(
	args value.Dict, bootargs map[string]interface{}, cache map[string]interface{},
) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return predictOperator{tr}, nil
}

func (pop predictOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "predict").
		Input([]value.Type{value.Types.Any}).
		Param("features", value.Types.List, false, false, value.Nil).
		Param("model_name", value.Types.String, true, false, value.Nil).
		Param("model_version", value.Types.String, true, false, value.Nil)
}

func (pop predictOperator) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	var featureVecs []value.List
	for in.HasMore() {
		_, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		features, _ := contextKwargs.Get("features")
		featureVecs = append(featureVecs, features.(value.List))
	}
	modelName := staticKwargs.GetUnsafe("model_name").(value.String)
	modelVersion := staticKwargs.GetUnsafe("model_version").(value.String)
	// TODO: Split into correctly sized requests instead of just 1.
	res, err := pop.tier.SagemakerClient.Score(context.TODO(), &sagemaker.ScoreRequest{
		ModelName:    string(modelName),
		ModelVersion: string(modelVersion),
		FeatureLists: featureVecs,
	})
	if err != nil {
		return err
	}
	for _, score := range res.Scores {
		out.Append(score)
	}
	return nil
}
