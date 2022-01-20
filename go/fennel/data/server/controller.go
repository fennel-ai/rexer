package main

import (
	"fennel/instance"
	"fennel/kafka"
	"fennel/test"
)

type MainController struct {
	instance     instance.Instance
	counterTable CounterTable
	producer     kafka.FProducer
	consumer     kafka.FConsumer
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
	producer, consumer, err := kafka.DefaultProducerConsumer(ACTIONLOG_TOPICNAME)
	if err != nil {
		return MainController{}, err
	}
	return MainController{
		instance:     this,
		counterTable: counterTable,
		producer:     producer,
		consumer:     consumer,
	}, nil
}
