package opdefs

import (
	"encoding/json"
	"fennel/engine/operators"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
	To fix this test please look at go/fennel/opdefs/generate/generate_client_operators.go
*/

func TestFeatureLog_Apply(t *testing.T) {
	clientOperators, err := os.ReadFile("../../../../rexer-pyclient-alpha/rexerclient/gen/operators.txt")
	assert.NoError(t, err)
	ops := operators.GetOperators()
	if err != nil {
		log.Fatalf("Failed to fetch the operators: %s", err)
	}
	opsIndented, err := json.MarshalIndent(ops, "", " \t")
	if err != nil {
		log.Fatalf("failed marshalling to json: %s", err)
	}
	assert.Equal(t, opsIndented, clientOperators)
}
