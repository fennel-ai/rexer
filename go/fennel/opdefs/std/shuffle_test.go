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
	copy(outTable, intable)
	optest.AssertElementsMatch(t, tr, &ShuffleOperator{}, staticKwargs, [][]value.Value{intable}, contextKwargs, outTable)
}
