//go:build !badger

package tailer

import "fennel/tier"

func Run(tier tier.Tier) error {
	return nil
}
