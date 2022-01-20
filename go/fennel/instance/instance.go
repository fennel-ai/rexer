package instance

import (
	"fennel/db"
	"fennel/redis"
	"flag"
	"fmt"
)

var instance = flag.String("instance", "test", "possible values are test(default), prod, dev")

type Type uint8

const (
	PROD Type = 1
	DEV       = 2
	TEST      = 3
)

func Current() Type {
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

func (i Type) Name() string {
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

type Instance struct {
	DB    db.Connection
	Redis redis.Client
}
