package s3

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient_UploadDelete(t *testing.T) {
	c := NewClient(S3Args{Region: "ap-south-1"})
	contents := "some random text"
	file := strings.NewReader(contents)
	fileName := "some_file.txt"
	bucketName := os.Getenv("MODEL_STORE_S3_BUCKET")

	err := c.Upload(file, fileName, bucketName)
	assert.NoError(t, err)

	found, err := c.Download(fileName, bucketName)
	assert.NoError(t, err)
	assert.Equal(t, string(found), contents)

	err = c.Delete(fileName, bucketName)
	assert.NoError(t, err)
}
