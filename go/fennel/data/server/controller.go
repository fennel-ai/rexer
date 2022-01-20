package main

import (
	"fennel/instance"
	"fennel/kafka"
	profileData "fennel/profile/data"
	"fennel/test"
)

type MainController struct {
	profile         profileData.Controller
	instance        instance.Instance
	counterTable    CounterTable
	checkpointTable CheckpointTable
	producer        kafka.FProducer
	consumer        kafka.FConsumer
}

// TODO: this whole function needs to move to main_test
// because it is creating default controller
func DefaultMainController() (MainController, error) {
	conn, err := test.DefaultDB()
	if err != nil {
		return MainController{}, err
	}
	this, err := test.DefaultInstance()
	if err != nil {
		return MainController{}, err
	}
	counterTable, err := NewCounterTable(conn)
	if err != nil {
		return MainController{}, err
	}
	checkpointTable, err := NewCheckpointTable(conn)
	if err != nil {
		return MainController{}, err
	}
	profileProvider, err := profileData.NewProfileTable(conn)
	if err != nil {
		return MainController{}, err
	}
	err = profileProvider.Init()
	if err != nil {
		return MainController{}, err
	}
	producer, consumer, err := kafka.DefaultProducerConsumer(ACTIONLOG_TOPICNAME)
	if err != nil {
		return MainController{}, err
	}
	return MainController{
		profile:         profileData.NewController(profileProvider),
		instance:        this,
		counterTable:    counterTable,
		checkpointTable: checkpointTable,
		producer:        producer,
		consumer:        consumer,
	}, nil
}
