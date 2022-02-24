package resource

import (
	"fmt"

	"fennel/lib/ftypes"
)

type Scope interface {
	ID() uint32
	PrefixedName(string) string
}

var _ Scope = TierScope{}
var _ Scope = MothershipScope{}

type TierScope struct {
	mothershipID uint32
	tierID       ftypes.TierID
}

func NewTierScope(mothershipID uint32, tierID ftypes.TierID) TierScope {
	return TierScope{
		mothershipID: mothershipID,
		tierID:       tierID,
	}
}

func (t TierScope) ID() uint32 {
	return uint32(t.tierID)
}

func (t TierScope) PrefixedName(name string) string {
	return fmt.Sprintf("t_%d_%s", t.tierID, name)
}

type MothershipScope struct {
	mothershipID uint32
}

func NewMothershipScope(mothershipID uint32) MothershipScope {
	return MothershipScope{
		mothershipID: mothershipID,
	}
}

func (m MothershipScope) ID() uint32 {
	return m.mothershipID
}

func (m MothershipScope) PrefixedName(name string) string {
	return fmt.Sprintf("m_%d_%s", m.mothershipID, name)
}
