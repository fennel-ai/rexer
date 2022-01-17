package main

import (
	"fennel/db"
	"fennel/kafka"
	profileData "fennel/profile/data"
)

type MainController struct {
	profile         profileData.Controller
	actionTable     ActionTable
	counterTable    CounterTable
	checkpointTable CheckpointTable
	producer        kafka.FProducer
	consumer        kafka.FConsumer
}

func DefaultMainController() (MainController, error) {
	DB, err := db.Default()
	if err != nil {
		return MainController{}, err
	}
	conn := DB.(db.Connection)
	actionTable, err := NewActionTable(conn)
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
		actionTable:     actionTable,
		counterTable:    counterTable,
		checkpointTable: checkpointTable,
		producer:        producer,
		consumer:        consumer,
	}, nil
}
