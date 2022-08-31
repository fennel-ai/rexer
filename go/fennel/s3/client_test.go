//go:build sagemaker

package s3

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	c := NewClient(S3Args{Region: "ap-south-1"})
	contents := "some random text"
	file := strings.NewReader(contents)
	fileName := "some_file.txt"
	bucketName := "my-xgboost-test-bucket-2"

	err := c.Upload(file, fileName, bucketName)
	assert.NoError(t, err)

	exist, err := c.Exist(fileName, bucketName)
	assert.NoError(t, err)
	assert.Equal(t, exist, true)

	found, err := c.Download(fileName, bucketName)
	assert.NoError(t, err)
	assert.Equal(t, string(found), contents)

	err = c.Delete(fileName, bucketName)
	assert.NoError(t, err)

	exist, err = c.Exist(fileName, bucketName)
	assert.NoError(t, err)
	assert.Equal(t, exist, false)
}
