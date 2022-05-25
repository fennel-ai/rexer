package modelstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModelStore(t *testing.T) {
	ms := NewModelStore(ModelStoreArgs{
		ModelStoreS3Bucket:     "my-xgboost-test-bucket-2",
		ModelStoreEndpointName: "integration-test-endpoint",
	}, 1)
	assert.Equal(t, "s3://my-xgboost-test-bucket-2/f.txt", ms.GetArtifactPath("f.txt"))
}
