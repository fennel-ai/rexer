package sagemaker

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	lib "fennel/lib/sagemaker"
	"fennel/lib/value"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sagemakerruntime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Since the retry delays will add up to the overall latency, we try to keep this as low as possible
const maxInvokeRetries = 3
const initialDelay = 200 * time.Millisecond

var (
	invokeRetries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sagemaker_invoke_retries",
			Help: "Number of sagemaker invoke retries due to throttle",
		}, []string{"errorCode", "framework"},
	)
	invocationInputSize = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name: "sagemaker_invocation_input_size",
		Help: "Size of sagemaker invocation input",
		Objectives: map[float64]float64{
			0.25: 0.05,
			0.50: 0.05,
			0.75: 0.05,
			0.90: 0.05,
			0.95: 0.02,
			0.99: 0.01,
		},
	}, []string{"container"})
	invocationOutputSize = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name: "sagemaker_invocation_output_size",
		Help: "Size of sagemaker invocation output",
		Objectives: map[float64]float64{
			0.25: 0.05,
			0.50: 0.05,
			0.75: 0.05,
			0.90: 0.05,
			0.95: 0.02,
			0.99: 0.01,
		},
	}, []string{"container"})
	containerNameCtr = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sagemaker_containername_ctr",
			Help: "Sagemaker endpoint invocations",
		}, []string{"endpoint", "containername"},
	)
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

func invokeRetryOnThrottle(ctx context.Context, framework string, client *sagemakerruntime.SageMakerRuntime, input *sagemakerruntime.InvokeEndpointInput) (*sagemakerruntime.InvokeEndpointOutput, error) {
	var out *sagemakerruntime.InvokeEndpointOutput
	delay := initialDelay
	for i := 0; i < maxInvokeRetries; i++ {
		var err error
		out, err = client.InvokeEndpointWithContext(ctx, input)
		if err == nil {
			break
		}
		// check if this is a ThrottleException, if so, retry with a backoff
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == "ThrottlingException" {
				// we should backoff here and retry
				invokeRetries.WithLabelValues("ThrottleException", framework).Inc()
				time.Sleep(delay)
				delay = delay * 2
				continue
			}
		}
		// some other error? Return it as is
		return nil, err
	}
	return out, nil
}

func (xga XGBoostAdapter) Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error) {
	payload := bytes.Buffer{}
	var contentType string
	// if features are stored as a dict, then use libsvm format otherwise use csv format
	containerNameCtr.WithLabelValues(in.EndpointName, in.ContainerName).Inc()
	if _, ok := in.ModelInput.(value.Dict); ok {
		contentType = "text/libsvm"
		for _, v := range in.ModelInput.(value.Dict).Iter() {
			payload.WriteRune('0')
			vd, ok := v.(value.Dict)
			if !ok {
				return nil, fmt.Errorf("expected dict but found: '%s'", v.String())
			}
			// libsvm expects keys in ascending order, so we sort first
			type kvpair struct {
				key uint64
				val value.Value
			}
			features := make([]kvpair, len(vd.Iter()))
			i := 0
			for k, v := range vd.Iter() {
				key, err := strconv.ParseUint(k, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("expected key in feature dict to be an unsigned integer but found: '%s'", k)
				}
				features[i].key = key
				switch val := v.(type) {
				case value.Int, value.Double:
					features[i].val = val
				default:
					return nil, fmt.Errorf("expected value in feature dict to be number but found: '%s'", v.String())
				}
				i++
			}
			sort.SliceStable(features, func(i, j int) bool {
				return features[i].key < features[j].key
			})
			for _, f := range features {
				payload.WriteRune(' ')
				payload.WriteString(strconv.FormatUint(f.key, 10))
				payload.WriteRune(':')
				payload.WriteString(f.val.String())
			}
			payload.WriteRune('\n')
		}
	} else {
		contentType = "text/csv"
		for _, v := range in.ModelInput.(value.List).Values() {
			vl, ok := v.(value.List)
			if !ok {
				return nil, fmt.Errorf("expected list but found: '%s'", v.String())
			}
			for i := 0; i < vl.Len(); i++ {
				v, _ := vl.At(i)
				payload.WriteString(v.String())
				if i != vl.Len()-1 {
					payload.WriteRune(',')
				}
			}
			payload.WriteRune('\n')
		}
	}
	payloadBuf := payload.Bytes()
	invocationInputSize.WithLabelValues(in.ContainerName).Observe(float64(len(payload.Bytes())))
	out, err := invokeRetryOnThrottle(ctx, "xgboost", xga.client, &sagemakerruntime.InvokeEndpointInput{
		Body:                    payloadBuf,
		ContentType:             aws.String(contentType),
		EndpointName:            aws.String(in.EndpointName),
		TargetContainerHostname: aws.String(in.ContainerName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to invoke sagemaker endpoint: %v", err)
	}
	invocationOutputSize.WithLabelValues(in.ContainerName).Observe(float64(len(out.Body)))
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
	payload := value.ToJSON(in.ModelInput)
	out, err := invokeRetryOnThrottle(ctx, "sklearn", sa.client, &sagemakerruntime.InvokeEndpointInput{
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
	payload := value.ToJSON(in.ModelInput)
	invokeEndpointInput := &sagemakerruntime.InvokeEndpointInput{
		Body:         payload,
		ContentType:  aws.String("application/json"),
		EndpointName: aws.String(in.EndpointName),
	}
	if in.ContainerName != "" {
		invokeEndpointInput.TargetContainerHostname = aws.String(in.ContainerName)
	}
	out, err := invokeRetryOnThrottle(ctx, "pytorch", pta.client, invokeEndpointInput)
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
	inputs := value.NewList()
	inputs.Grow(in.ModelInput.(value.List).Len())
	// It is expected that every feature list only contains one feature, a string, which is the input to the model.
	for _, v := range in.ModelInput.(value.List).Values() {
		inputs.Append(value.String(removeSpecialCharacters(v.String())))
	}

	payload := value.ToJSON(value.NewDict(map[string]value.Value{"inputs": inputs}))

	out, err := invokeRetryOnThrottle(ctx, "huggingface", hfa.client, &sagemakerruntime.InvokeEndpointInput{
		Body:         payload,
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
	payload := value.ToJSON(in.ModelInput)
	out, err := invokeRetryOnThrottle(ctx, "tensorflow", tfa.client, &sagemakerruntime.InvokeEndpointInput{
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

func fromCSV(csv []byte) ([]value.Value, error) {
	rows := bytes.Split(csv, []byte("\n"))
	rows = rows[:len(rows)-1] // ignore empty last line
	if len(rows) == 0 {
		rows = bytes.Split(csv, []byte(","))
	}
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
