package embedding

import (
	"context"
	lib "fennel/lib/sagemaker"
	"fmt"
	"strings"
	"sync"

	modelstore "fennel/controller/modelstore"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	operators.Register(pretrainedModel{})
}

type pretrainedModel struct {
	tier tier.Tier
}

func (p pretrainedModel) New(args value.Dict, bootargs map[string]interface{}, cache *sync.Map) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return pretrainedModel{tr}, nil
}

func (p pretrainedModel) Apply(ctx context.Context, kwargs value.Dict, in operators.InputIter, outs *value.List) error {
	var rows []value.Value
	inputs := make([]value.List, 0)
	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}

		input, ok := contextKwargs.Get("input")
		if !ok || input == value.Nil {
			input = heads[0]
		}
		inputs = append(inputs, value.NewList(input))
		rows = append(rows, heads[0])
	}
	model_type := string(get(kwargs, "model").(value.String))
	modelConfig, ok := modelstore.SupportedPretrainedModels[model_type]
	if !ok {
		return fmt.Errorf("Pretrained model %s is not supported, currently supported models are : %s", model_type, strings.Join(modelstore.GetSupportedModels(), ", "))
	}

	req := lib.ScoreRequest{
		Framework:    modelConfig.Framework,
		EndpointName: modelstore.PreTrainedModelId(model_type, p.tier.ID),
		FeatureLists: inputs,
	}
	res, err := p.tier.SagemakerClient.Score(ctx, &req)

	if err != nil {
		return err
	}
	field := string(get(kwargs, "field").(value.String))
	outs.Grow(len(rows))
	for i, row := range rows {
		var out value.Value
		vector := res.Scores[i]
		if len(field) > 0 {
			d := row.(value.Dict)
			d.Set(field, vector)
			out = d
		} else {
			out = vector
		}
		outs.Append(out)
	}
	return nil
}

func (p pretrainedModel) Signature() *operators.Signature {
	return operators.NewSignature("embedding", "model").
		Input([]value.Type{value.Types.Dict}).
		ParamWithHelp("field", value.Types.String, true, true, value.String(""), "StaticKwarg: String param that is used as key post evaluation of this operator").
		ParamWithHelp("model", value.Types.String, true, false, nil, "model name that should be called for eg sbert").
		ParamWithHelp("input", value.Types.String, false, true, nil, "ContextKwarg: Expr that is evaluated to provide input to the model.")
}

var _ operators.Operator = &pretrainedModel{}

func get(d value.Dict, k string) value.Value {
	ret, _ := d.Get(k)
	return ret
}
