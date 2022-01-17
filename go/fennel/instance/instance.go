package instance

import (
	"flag"
	"fmt"
)

var instance = flag.String("instance", "test", "possible values are test(default), prod, dev")

type Instance uint8

const (
	PROD Instance = 1
	DEV           = 2
	TEST          = 3
)

func Current() Instance {
	switch *instance {
	case "test":
		return TEST
	case "prod":
		return PROD
	case "dev":
		return DEV
	default:
		panic(fmt.Sprintf("invalid instance flag passed: %v", *instance))
	}
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
