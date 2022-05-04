package phaser

import "fennel/tier"

type Phaser struct {
	tier tier.Tier
}

func NewPhaser(tier tier.Tier) *Phaser {
	return &Phaser{
		tier: tier,
	}
}
