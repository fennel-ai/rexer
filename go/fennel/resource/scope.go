package resource

import (
	"fmt"

	"fennel/lib/ftypes"
)

type Scope interface {
	ID() ftypes.RealmID
	PrefixedName(string) string
}

var _ Scope = TierScope{}
var _ Scope = MothershipScope{}

type TierScope struct {
	tierID ftypes.RealmID
}

func NewTierScope(tierID ftypes.RealmID) TierScope {
	return TierScope{
		tierID: tierID,
	}
}

func (t TierScope) ID() ftypes.RealmID {
	return t.tierID
}

func (t TierScope) PrefixedName(name string) string {
	return fmt.Sprintf("t_%d_%s", t.tierID, name)
}

type MothershipScope struct {
	mothershipID ftypes.RealmID
}

// NewMothershipScope returns a new scope object with 10^9 added to its ID.
// MothershipIDs must be between 10^9 and 2*(10^9)-1 inclusive.
func NewMothershipScope(mothershipID ftypes.RealmID) MothershipScope {
	return MothershipScope{
		mothershipID: 1e9 + mothershipID,
	}
}

func (m MothershipScope) ID() ftypes.RealmID {
	return m.mothershipID
}

func (m MothershipScope) PrefixedName(name string) string {
	return fmt.Sprintf("m_%d_%s", m.mothershipID, name)
}
