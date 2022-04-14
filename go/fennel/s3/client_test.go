package s3

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient_UploadDelete(t *testing.T) {
	c := NewClient(S3Args{Region: "ap-south-1"})
	file := strings.NewReader("some random text")
	fileName := "some_file.txt"
	bucketName := os.Getenv("MODEL_STORE_S3_BUCKET")

	err := c.UploadModelToS3(file, fileName, bucketName)
	assert.NoError(t, err)

	err = c.DeleteModelFromS3(fileName, bucketName)
	assert.NoError(t, err)
}
