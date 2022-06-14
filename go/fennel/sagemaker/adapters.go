package sagemaker

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"unicode"

	lib "fennel/lib/sagemaker"
	"fennel/lib/value"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sagemakerruntime"
)

type Adapter interface {
	Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error)
}

var _ Adapter = XGBoostAdapter{}
var _ Adapter = SklearnAdapter{}
var _ Adapter = TensorFlowAdapter{}
var _ Adapter = PyTorchAdapter{}
var _ Adapter = HuggingFaceAdapter{}

func (smc SMClient) getAdapter(framework string) (Adapter, error) {
	switch framework {
	case "xgboost":
		return XGBoostAdapter{client: smc.runtimeClient}, nil
	case "sklearn":
		return SklearnAdapter{client: smc.runtimeClient}, nil
	case "tensorflow":
		return TensorFlowAdapter{client: smc.runtimeClient}, nil
	case "pytorch":
		return PyTorchAdapter{client: smc.runtimeClient}, nil
	case "huggingface":
		return HuggingFaceAdapter{client: smc.runtimeClient}, nil
	default:
		return nil, fmt.Errorf("unsupported framework")
	}
}

type XGBoostAdapter struct {
	client *sagemakerruntime.SageMakerRuntime
}

func (xga XGBoostAdapter) Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error) {
	if len(in.FeatureLists) == 0 {
		return &lib.ScoreResponse{}, nil
	}
	payload := bytes.Buffer{}
	for _, fl := range in.FeatureLists {
		sf := make([]string, 0, fl.Len())
		for i := 0; i < fl.Len(); i++ {
			f, _ := fl.At(i)
			raw, err := f.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal feaures: %v: %v", f, err)
			}
			sf = append(sf, string(raw))
		}
		line := strings.Join(sf, ",")
		payload.Write([]byte(line[1 : len(line)-1]))
		_, err := payload.WriteRune('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to write newline: %v", err)
		}
	}
	var contentType string
	// If features list if empty, assume input is csv.
	if in.FeatureLists[0].Len() == 0 {
		contentType = "text/csv"
	} else {
		feature, _ := in.FeatureLists[0].At(0)
		if _, ok := feature.(value.Double); ok {
			contentType = "text/csv"
		} else if _, ok := feature.(value.Int); ok {
			contentType = "text/csv"
		} else {
			contentType = "text/libsvm"
		}
	}
	out, err := xga.client.InvokeEndpointWithContext(ctx, &sagemakerruntime.InvokeEndpointInput{
		Body:                    payload.Bytes(),
		ContentType:             aws.String(contentType),
		EndpointName:            aws.String(in.EndpointName),
		TargetContainerHostname: aws.String(in.ContainerName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to invoke sagemaker endpoint: %v", err)
	}
	scores, err := fromCSV(out.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}
	return &lib.ScoreResponse{
		Scores: scores,
	}, nil
}

type SklearnAdapter struct {
	client *sagemakerruntime.SageMakerRuntime
}

func (sa SklearnAdapter) Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error) {
	if len(in.FeatureLists) == 0 {
		return &lib.ScoreResponse{}, nil
	}
	payload := toJSON(in.FeatureLists)
	out, err := sa.client.InvokeEndpointWithContext(ctx, &sagemakerruntime.InvokeEndpointInput{
		Body:                    payload,
		ContentType:             aws.String("application/json"),
		EndpointName:            aws.String(in.EndpointName),
		TargetContainerHostname: aws.String(in.ContainerName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to invoke sagemaker endpoint: %v", err)
	}
	response, err := value.FromJSON(out.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reponse as JSON: %v", err)
	}
	rList, ok := response.(value.List)
	if !ok {
		return nil, fmt.Errorf("expected response to be a value list but found: '%s'", response.String())
	}
	return &lib.ScoreResponse{Scores: rList.Values()}, nil
}

type PyTorchAdapter struct {
	client *sagemakerruntime.SageMakerRuntime
}

func (pta PyTorchAdapter) Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error) {
	if len(in.FeatureLists) == 0 {
		return &lib.ScoreResponse{}, nil
	}
	payload := toJSON(in.FeatureLists)
	out, err := pta.client.InvokeEndpointWithContext(ctx, &sagemakerruntime.InvokeEndpointInput{
		Body:                    []byte(payload),
		ContentType:             aws.String("application/json"),
		EndpointName:            aws.String(in.EndpointName),
		TargetContainerHostname: aws.String(in.ContainerName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to invoke sagemaker endpoint: %v", err)
	}
	response, err := value.FromJSON(out.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reponse as JSON: %v", err)
	}
	rList, ok := response.(value.List)
	if !ok {
		return nil, fmt.Errorf("expected response to be a value list but found: '%v'", response.String())
	}
	return &lib.ScoreResponse{Scores: rList.Values()}, nil
}

type HuggingFaceAdapter struct {
	client *sagemakerruntime.SageMakerRuntime
}

func removeSpecialCharacters(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsDigit(r) || unicode.IsLetter(r) || unicode.IsSpace(r) || r == ',' || r == '.' {
			return r
		}
		return -1
	}, s)
}

func (hfa HuggingFaceAdapter) Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error) {
	if len(in.FeatureLists) == 0 {
		return &lib.ScoreResponse{}, nil
	}
	inputs := value.NewList()
	inputs.Grow(len(in.FeatureLists))
	// It is expected that every feature list only contains one feature, a string, which is the input to the model.
	for _, v := range in.FeatureLists {
		inp, err := v.At(0)
		if err != nil {
			return nil, fmt.Errorf("failed to get input: %v", err)
		}
		inputs.Append(value.String(removeSpecialCharacters(inp.String())))
	}

	payload := value.ToJSON(value.NewDict(map[string]value.Value{"inputs": inputs}))

	out, err := hfa.client.InvokeEndpointWithContext(ctx, &sagemakerruntime.InvokeEndpointInput{
		Body:         []byte(payload),
		ContentType:  aws.String("application/json"),
		EndpointName: aws.String(in.EndpointName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to invoke sagemaker endpoint: %v", err)
	}
	response, err := value.FromJSON(out.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reponse as JSON: %v", err)
	}
	rDict, ok := response.(value.Dict)
	if !ok {
		return nil, fmt.Errorf("expected response to be a value list but found: '%v'", response.String())
	}
	vectors, _ := rDict.Get("vectors")
	rList, ok := vectors.(value.List)
	if !ok {
		return nil, fmt.Errorf("expected response to be a value list but found: '%v'", response.String())
	}
	return &lib.ScoreResponse{Scores: rList.Values()}, nil
}

type TensorFlowAdapter struct {
	client *sagemakerruntime.SageMakerRuntime
}

func (tfa TensorFlowAdapter) Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error) {
	if len(in.FeatureLists) == 0 {
		return &lib.ScoreResponse{}, nil
	}
	payload := toJSON(in.FeatureLists)
	out, err := tfa.client.InvokeEndpointWithContext(ctx, &sagemakerruntime.InvokeEndpointInput{
		Body:                    payload,
		ContentType:             aws.String("application/json"),
		EndpointName:            aws.String(in.EndpointName),
		TargetContainerHostname: aws.String(in.ContainerName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to invoke sagemaker endpoint: %v", err)
	}
	response, err := value.FromJSON(out.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reponse as JSON: %v", err)
	}
	rDict, ok := response.(value.Dict)
	if !ok {
		return nil, fmt.Errorf("expected response to be a value dict but found: '%v'", response.String())
	}
	predictions, ok := rDict.Get("predictions")
	if !ok {
		return nil, fmt.Errorf("failed to find key 'predictions' in response dictionary: '%v'", rDict.String())
	}
	pList, ok := predictions.(value.List)
	if !ok {
		return nil, fmt.Errorf("expected predictions to be a value list but found: '%v'", predictions.String())
	}
	return &lib.ScoreResponse{Scores: pList.Values()}, nil
}

func toJSON(featureLists []value.List) []byte {
	fLists := value.NewList()
	fLists.Grow(len(featureLists))
	for _, fl := range featureLists {
		fLists.Append(fl)
	}
	return value.ToJSON(fLists)
}

func fromCSV(csv []byte) ([]value.Value, error) {
	rows := bytes.Split(csv, []byte("\n"))
	rows = rows[:len(rows)-1] // ignore empty last line
	vals := make([]value.Value, len(rows))
	for i, row := range rows {
		v, err := value.FromJSON(row)
		if err != nil {
			return nil, fmt.Errorf("failed to parse csv: %v", err)
		}
		vals[i] = v
	}
	return vals, nil
}
