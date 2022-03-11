package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestShuffleOperator_Apply(t *testing.T) {
	intable := []value.Dict{
		{"name": value.String("first")},
		{"name": value.String("second")},
		{"name": value.String("third")},
	}
	staticKwargs := value.Dict{}
	contextKwargs := []value.Dict{{}, {}, {}}

	tr := tier.Tier{}
	optest.AssertElementsMatch(t, tr, &ShuffleOperator{}, staticKwargs, intable, contextKwargs, intable)
}
