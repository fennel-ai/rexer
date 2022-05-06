//go:build sagemaker

package sagemaker

import (
	"context"
	"math/rand"
	"testing"

	lib "fennel/lib/sagemaker"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestAdapterScore(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	frameworks := []string{"xgboost", "sklearn", "tensorflow", "pytorch"}
	var featureVectors []value.List
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
			ContainerName: lib.GetContainerName(framework+"-test", "v1"),
			Framework:     framework,
			FeatureLists:  featureVectors,
		})
		assert.NoError(t, err)
		scores := resp.Scores
		assert.Equal(t, 10, len(scores))
		for _, score := range scores {
			v, ok := score.(value.List)
			assert.True(t, ok)
			assert.Equal(t, 4, v.Len())
		}
	}
}
