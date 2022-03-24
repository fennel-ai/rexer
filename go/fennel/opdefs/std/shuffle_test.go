package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestShuffleOperator_Apply(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"name": value.String("first")}),
		value.NewDict(map[string]value.Value{"name": value.String("second")}),
		value.NewDict(map[string]value.Value{"name": value.String("third")}),
	}
	staticKwargs := value.Dict{}
	contextKwargs := []value.Dict{{}, {}, {}}

	tr := tier.Tier{}
	outTable := make([]value.Value, len(intable))
	for i, in := range intable {
		outTable[i] = in.Clone()
	}
	optest.AssertElementsMatch(t, tr, &ShuffleOperator{}, staticKwargs, intable, contextKwargs, outTable)
}
