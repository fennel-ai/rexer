package resource

import (
	"fennel/lib/ftypes"
	"fmt"
	"strconv"
)

type Type uint8

/*
Resource represents any external resource that needs
to be initialized/closed with some dependency management.
The way to define any new resource is to create a struct that
implements Config interface. Using that config, materialize the
resource. Any initialization/setup should be done during this
materialization.

*/

const (
	DBConnection  Type = 1
	RedisClient        = 2
	KafkaConsumer      = 3
	KafkaProducer      = 4
)

type Config interface {
	Materialize(scope Scope) (Resource, error)
}

type Resource interface {
	Close() error
	Type() Type
}

type Scope struct {
	path []string
}

// GetTierID returns 0 (invalid tier) when scope has no TierID.
func (s *Scope) GetTierID() ftypes.TierID {
	if len(s.path) < 1 {
		return 0
	}
	t, err := strconv.ParseUint(s.path[0], 10, 64)
	if err != nil {
		return 0
	}
	return ftypes.TierID(t)
}

func TieredName(tierID ftypes.TierID, name string) string {
	return fmt.Sprintf("t_%d_%s", tierID, name)
}

func GetTierScope(id ftypes.TierID) Scope {
	return Scope{path: []string{fmt.Sprintf("%d", id)}}
}

func GetMothershipScope() Scope {
	return Scope{path: []string{}}
}
