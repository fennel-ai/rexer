package instance

import (
	"fmt"
)

type Resource uint8

const (
	DB    Resource = 1
	Kafka          = 2
)

type Instance uint8

const (
	PROD Instance = 1
	DEV           = 2
	TEST          = 3
)

func Current() Instance {
	return TEST
}

func (i Instance) Name() string {
	switch i {
	case PROD:
		return "prod"
	case DEV:
		return "dev"
	case TEST:
		return "test"
	default:
		panic(fmt.Sprintf("unexpected instance: %v", i))
	}
}

type SetupFn func() error

var fnmap map[Resource][]SetupFn

func init() {
	fnmap = make(map[Resource][]SetupFn, 0)
}

func Register(key Resource, fn SetupFn) {
	v, ok := fnmap[key]
	if !ok {
		v = make([]SetupFn, 0)
	}
	v = append(v, fn)
	fnmap[key] = v
}

// Setup runs all setups functions
// if include is non-empty, only those keys are considered
func Setup(include []Resource) error {
	for k, fns := range fnmap {
		if len(include) > 0 && !contains(include, k) {
			continue
		}
		for _, fn := range fns {
			err := fn()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func contains(s []Resource, k Resource) bool {
	for _, a := range s {
		if a == k {
			return true
		}
	}
	return false
}
