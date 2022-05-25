package model

import (
	"context"
	"sync"

	modelstore "fennel/controller/modelstore"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	operators.Register(predictOperator{})
}

type predictOperator struct {
	tier tier.Tier
}

func (p predictOperator) New(args value.Dict, bootargs map[string]interface{}, cache *sync.Map) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return predictOperator{tr}, nil
}

func (p predictOperator) Apply(ctx context.Context, staticKwargs value.Dict, in operators.InputIter, outs *value.List) error {
	var rows []value.Value
	var inputs []value.List
	modelName := string(get(staticKwargs, "model").(value.String))
	_, isPretrainedModel := modelstore.SupportedPretrainedModels[modelName]

	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}

		input, ok := contextKwargs.Get("input")
		if !ok || input == value.Nil {
			input = heads[0]
		}
		inputs = append(inputs, input.(value.List))
		rows = append(rows, heads[0])
	}
	var outputs []value.Value
	var err error

	if isPretrainedModel {
		outputs, err = modelstore.PreTrainedScore(ctx, p.tier, modelName, inputs)
	} else {
		modelVersion := staticKwargs.GetUnsafe("version").(value.String)
		// TODO: Split into correctly sized requests instead of just 1.
		outputs, err = modelstore.Score(ctx, p.tier, string(modelName), string(modelVersion), inputs)
	}

	if err != nil {
		return err
	}
	field := string(get(staticKwargs, "field").(value.String))
	outs.Grow(len(rows))
	for i, row := range rows {
		var out value.Value
		result := outputs[i]
		if len(field) > 0 {
			d := row.(value.Dict)
			d.Set(field, result)
			out = d
		} else {
			out = result
		}
		outs.Append(out)
	}
	return nil
}

func (p predictOperator) Signature() *operators.Signature {
	return operators.NewSignature("model", "predict").
		Input([]value.Type{value.Types.Dict}).
		ParamWithHelp("field", value.Types.String, true, true, value.String(""), "StaticKwarg: String param that is used as key post evaluation of this operator").
		ParamWithHelp("model", value.Types.String, true, false, value.Nil, "model name that should be called for eg sbert").
		ParamWithHelp("input", value.Types.List, false, false, value.Nil, "ContextKwarg: Expr that is evaluated to provide input to the model.").
		ParamWithHelp("version", value.Types.String, true, true, value.String(""), "StaticKwarg: Model version that should be called for a given model. Not applicable for pretrained models.")
}

var _ operators.Operator = &predictOperator{}

func get(d value.Dict, k string) value.Value {
	ret, _ := d.Get(k)
	return ret
}
