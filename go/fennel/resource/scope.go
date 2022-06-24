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

type PlaneScope struct {
	planeId ftypes.RealmID
}

func NewPlaneScope(planeID ftypes.RealmID) PlaneScope {
	return PlaneScope{
		planeId: planeID,
	}
}

func (p PlaneScope) ID() ftypes.RealmID {
	return p.planeId
}

func (p PlaneScope) PrefixedName(name string) string {
	return fmt.Sprintf("p_%d_%s", p.planeId, name)
}

type MothershipScope struct {
	mothershipID ftypes.RealmID
}

func NewMothershipScope(mothershipID ftypes.RealmID) MothershipScope {
	return MothershipScope{
		mothershipID: mothershipID,
	}
}

func (m MothershipScope) ID() ftypes.RealmID {
	return 1e9 + m.mothershipID
}

func (m MothershipScope) PrefixedName(name string) string {
	return fmt.Sprintf("m_%d_%s", m.mothershipID, name)
}
