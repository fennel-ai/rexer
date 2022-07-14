//go:build sagemaker

package sagemaker

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	lib "fennel/lib/sagemaker"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestAdapterScore(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	frameworks := []string{"sklearn", "tensorflow", "pytorch"}
	var featureVectors []value.Value
	for i := 0; i < 10; i++ {
		fv := value.NewList()
		for j := 0; j < 64; j++ {
			fv.Append(value.Double(rand.Float64()))
		}
		featureVectors = append(featureVectors, fv)
	}
	for _, framework := range frameworks {
		resp, err := c.Score(context.Background(), &lib.ScoreRequest{
			EndpointName:  "frameworks-test-endpoint",
			ContainerName: "Container-" + framework + "-test-v1",
			Framework:     framework,
			FeaturesList:  featureVectors,
		})
		assert.NoError(t, err)
		scores := resp.Scores
		assert.Equal(t, 10, len(scores))
		for _, score := range scores {
			v, ok := score.(value.List)
			assert.True(t, ok, framework)
			assert.Equal(t, 4, v.Len(), framework)
		}
	}
}

func TestXgboost(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	// first test with featureList
	var featureVectors []value.Value
	for i := 0; i < 10; i++ {
		fv := value.NewList()
		for j := 0; j < 64; j++ {
			fv.Append(value.Double(rand.Float64()))
		}
		featureVectors = append(featureVectors, fv)
	}
	resp, err := c.Score(context.Background(), &lib.ScoreRequest{
		EndpointName:  "frameworks-test-endpoint",
		ContainerName: "Container-xgboost-test-v1",
		Framework:     "xgboost",
		FeaturesList:  featureVectors,
	})
	assert.NoError(t, err)
	scores := resp.Scores
	assert.Equal(t, 10, len(scores))
	for _, score := range scores {
		_, ok := score.(value.Double)
		assert.True(t, ok)
	}

	// now test with featureDict
	for i := 0; i < 10; i++ {
		fd := value.NewDict(nil)
		for j := 0; j < rand.Intn(64); j++ {
			key := rand.Intn(64)
			fd.Set(strconv.FormatInt(int64(key), 10), value.Double(rand.Float64()))
		}
		featureVectors[i] = fd
	}

	resp, err = c.Score(context.Background(), &lib.ScoreRequest{
		EndpointName:  "frameworks-test-endpoint",
		ContainerName: lib.GetContainerName("xgboost-test", "v1"),
		Framework:     "xgboost",
		FeaturesList:  featureVectors,
	})
	assert.NoError(t, err)
	scores = resp.Scores
	assert.Equal(t, 10, len(scores))
	for _, score := range scores {
		_, ok := score.(value.Double)
		assert.True(t, ok)
	}
}
