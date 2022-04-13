package sagemaker

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSMClient_S3UploadDelete(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	file := strings.NewReader("some random text")
	fileName := "some_file.txt"

	err = c.UploadModelToS3(file, fileName)
	assert.NoError(t, err)

	err = c.DeleteModelFromS3(fileName)
	assert.NoError(t, err)
}
