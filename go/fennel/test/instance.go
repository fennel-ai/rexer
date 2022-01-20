package test

import (
	"fennel/instance"
	"fennel/redis"
)

func DefaultInstance() (instance.Instance, error) {
	db, err := DefaultDB()
	if err != nil {
		return instance.Instance{}, err
	}
	resource, err := DefaultRedis()
	if err != nil {
		return instance.Instance{}, err
	}
	redClient := resource.(redis.Client)
	return instance.Instance{DB: db, Redis: redClient}, nil
}
