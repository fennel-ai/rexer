package vae

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestPipeGetDefinition(t *testing.T) {
	data, err := GetPipelineDefinition("test")
	assert.NoError(t, err)
	assert.NotEmpty(t, data)
	expected := `{
     "Version": "2020-12-01",
     "Metadata": {},
     "Parameters": [
     {
         "Name": "TrainInstanceType",
          "Type": "String",
         "DefaultValue": "ml.g4dn.4xlarge"
      }
   ]
     }`
	assert.Equal(t, strings.ReplaceAll(expected, " ", ""), strings.ReplaceAll(string(data), " ", ""))
}
